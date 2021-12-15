import logging
import os
import threading

from typing import Any, Dict, cast

from assemblyline.common import forge
from assemblyline.common import log as al_log
from assemblyline.common.constants import SERVICE_STATE_HASH
from assemblyline.common.forge import CachedObject
from assemblyline.common.heuristics import HeuristicHandler
from assemblyline.common.version import BUILD_MINOR, FRAMEWORK_VERSION, SYSTEM_VERSION
from assemblyline.odm.models.heuristic import Heuristic
from assemblyline.remote.datatypes.counters import Counters
from assemblyline.remote.datatypes.events import EventSender
from assemblyline.remote.datatypes.hash import ExpiringHash
from assemblyline_core.dispatching.client import DispatchClient

config = forge.get_config()


def get_heuristics() -> Dict[str, Heuristic]:
    return {h.heur_id: h for h in STORAGE.list_all_heuristics()}

#################################################################
# Configuration

CLASSIFICATION = forge.get_classification()
DEBUG = config.ui.debug
VERSION = os.environ.get('ASSEMBLYLINE_VERSION', f"{FRAMEWORK_VERSION}.{SYSTEM_VERSION}.{BUILD_MINOR}.dev0")
AUTH_KEY = os.environ.get('SERVICE_API_KEY', 'ThisIsARandomAuthKey...ChangeMe!')

RATE_LIMITER = Counters(prefix="quota",
                        host=config.core.redis.nonpersistent.host,
                        port=config.core.redis.nonpersistent.port,
                        track_counters=True)

# End of Configuration
#################################################################

#################################################################
# Prepare loggers
config.logging.log_to_console = config.logging.log_to_console or DEBUG
al_log.init_logging('svc', config=config)

LOGGER = logging.getLogger('assemblyline.svc')

LOGGER.debug('Logger ready!')

# End of prepare logger
#################################################################

#################################################################
# Global instances
STORAGE = forge.get_datastore(config=config)
FILESTORE = forge.get_filestore(config=config)
EVENT_SENDER = EventSender('changes.services',
                           host=config.core.redis.nonpersistent.host,
                           port=config.core.redis.nonpersistent.port)
STATUS_TABLE = ExpiringHash(SERVICE_STATE_HASH, ttl=60*30)
DISPATCH_CLIENT = DispatchClient(STORAGE)
HEURISTICS = cast(Dict[str, Heuristic], CachedObject(get_heuristics, refresh=300))
HEURISTIC_HANDLER = HeuristicHandler(STORAGE)
TAG_SAFELISTER = CachedObject(forge.get_tag_safelister, kwargs=dict(log=LOGGER, config=config, datastore=STORAGE),
                              refresh=300)
LOCK = threading.Lock()
# End global
#################################################################
