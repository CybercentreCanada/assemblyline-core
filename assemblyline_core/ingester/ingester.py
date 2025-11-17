#!/usr/bin/env python
"""
Ingester

Ingester is responsible for monitoring for incoming submission requests,
sending submissions, waiting for submissions to complete, sending a message
to a notification queue as specified by the submission and, based on the
score received, possibly sending a message to indicate that an alert should
be created.
"""

import logging
import threading
import time
from os import environ
from random import random
from typing import Any, Iterable, List, Optional, Tuple

import elasticapm

from assemblyline import odm
from assemblyline.common import exceptions, forge, isotime
from assemblyline.common.constants import DROP_PRIORITY
from assemblyline.common.exceptions import get_stacktrace_info
from assemblyline.common.importing import load_module_by_path
from assemblyline.common.isotime import now, now_as_iso
from assemblyline.common.metrics import MetricsFactory
from assemblyline.common.postprocess import ActionWorker
from assemblyline.common.str_utils import dotdump, safe_str
from assemblyline.datastore.exceptions import DataStoreException
from assemblyline.filestore import CorruptedFileStoreException, FileStoreException
from assemblyline.odm.messages.ingest_heartbeat import Metrics
from assemblyline.odm.messages.submission import Submission as MessageSubmission
from assemblyline.odm.messages.submission import SubmissionMessage
from assemblyline.odm.models.alert import EXTENDED_SCAN_VALUES
from assemblyline.odm.models.filescore import FileScore
from assemblyline.odm.models.submission import Submission as DatabaseSubmission
from assemblyline.odm.models.submission import SubmissionParams
from assemblyline.odm.models.user import User
from assemblyline.remote.datatypes.events import EventWatcher
from assemblyline.remote.datatypes.hash import Hash
from assemblyline.remote.datatypes.queues.comms import CommsQueue
from assemblyline.remote.datatypes.queues.multi import MultiQueue
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.queues.priority import PriorityQueue
from assemblyline.remote.datatypes.user_quota_tracker import UserQuotaTracker
from assemblyline_core.dispatching.dispatcher import Dispatcher
from assemblyline_core.server_base import ThreadedCoreBase
from assemblyline_core.submission_client import SubmissionClient

from .constants import COMPLETE_QUEUE_NAME, INGEST_QUEUE_NAME, drop_chance

_dup_prefix = 'w-m-'
_notification_queue_prefix = 'nq-'
_max_retries = 10
_retry_delay = 60 * 4  # Wait 4 minutes to retry
_max_time = 2 * 24 * 60 * 60  # Wait 2 days for responses.
HOUR_IN_SECONDS = 60 * 60
COMPLETE_THREADS = int(environ.get('INGESTER_COMPLETE_THREADS', 4))
INGEST_THREADS = int(environ.get('INGESTER_INGEST_THREADS', 1))
SUBMIT_THREADS = int(environ.get('INGESTER_SUBMIT_THREADS', 4))


def must_drop(length: int, maximum: int) -> bool:
    """
    To calculate the probability of dropping an incoming submission we compare
    the number returned by random() which will be in the range [0,1) and the
    number returned by tanh() which will be in the range (-1,1).

    If length is less than maximum the number returned by tanh will be negative
    and so drop will always return False since the value returned by random()
    cannot be less than 0.

    If length is greater than maximum, drop will return False with a probability
    that increases as the distance between maximum and length increases:

        Length           Chance of Dropping

        <= maximum       0
        1.5 * maximum    0.76
        2 * maximum      0.96
        3 * maximum      0.999
    """
    return random() < drop_chance(length, maximum)


@odm.model()
class IngestTask(odm.Model):
    # Submission Parameters
    submission: MessageSubmission = odm.compound(MessageSubmission)

    # Shortcut for properties of the submission
    @property
    def file_size(self) -> int:
        return sum(file.size for file in self.submission.files)

    @property
    def params(self) -> SubmissionParams:
        return self.submission.params

    @property
    def sha256(self) -> str:
        return self.submission.files[0].sha256

    # Information about the ingestion itself, parameters irrelevant
    retries = odm.Integer(default=0)

    # Fields added after a submission is complete for notification/bookkeeping processes
    failure = odm.Text(default='')  # If the ingestion has failed for some reason, what is it?
    score = odm.Optional(odm.Integer())  # Score from previous processing of this file
    extended_scan = odm.Enum(EXTENDED_SCAN_VALUES, default="skipped")  # Status of the extended scan
    ingest_id = odm.UUID()  # Ingestion Identifier
    ingest_time = odm.Date(default="NOW")  # Time at which the file was ingested
    notify_time = odm.Optional(odm.Date())  # Time at which the user is notify the submission is finished
    to_ingest = odm.Boolean(default=False)

