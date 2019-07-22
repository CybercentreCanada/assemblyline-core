import hashlib

from assemblyline.common import forge
from assemblyline.common.classification import InvalidClassification
from assemblyline.common.caching import TimeExpiredCache
from assemblyline.common.dict_utils import recursive_update
from assemblyline.common.str_utils import safe_str
from assemblyline.common.isotime import now_as_iso
from assemblyline.odm.messages.alert import AlertMessage
from assemblyline.remote.datatypes.lock import Lock
from assemblyline.remote.datatypes.queues.comms import CommsQueue

CACHE_LEN = 60 * 60 * 24
CACHE_EXPIRY_RATE = 60

action_queue_map = None
cache = TimeExpiredCache(CACHE_LEN, CACHE_EXPIRY_RATE)
Classification = forge.get_classification()
config = forge.get_config()
summary_tags = (
    "av.virus_name", "attribution.exploit",
    "file.config", "technique.obfuscation", "file.behavior",
    "attribution.family", "attribution.implant", "network.domain", "network.ip",
    "technique.obfuscation", "attribution.actor",
)


def service_name_from_key(key):
    # noinspection PyBroadException
    try:
        return key.split('.')[1]
    except Exception:
        return ""


def get_submission_record(counter, datastore, sid):
    srecord = datastore.submission.get(sid, as_obj=False)

    if not srecord:
        counter.increment('error')
        raise Exception("Couldn't find submission: %s" % sid)

    if srecord.get('state', 'unknown') != 'completed':
        raise Exception("Submission not finalized: %s" % sid)

    return srecord


def get_summary(datastore, srecord):
    max_classification = srecord['classification']

    summary = {
        'av.virus_name': set(),
        'attribution.exploit': set(),
        'attribution': set(),
        'file.config': set(),
        'technique.obfuscation': set(),
        'file.behavior': set(),
        'file.rule.yara': set(),
        'attribution.family': set(),
        'attribution.implant': set(),
        'network.domain.static': set(),
        'network.domain.dynamic': set(),
        'network.ip.static': set(),
        'network.ip.dynamic': set(),
        'technique.config': set(),
        'attribution.actor': set()
    }

    for t in datastore.get_tag_list_from_keys(srecord.get('results', [])):
        tag_value = t['value']
        if tag_value == '':
            continue

        tag_context = t.get('context', None)
        tag_type = t['type']
        if tag_type in ('network.domain', 'network.ip'):
            if tag_context is None:
                tag_type += '.static'
            else:
                tag_type += '.dynamic'
        elif tag_type not in summary:
            continue

        sub_tag = {
            'attribution.exploit': 'EXP',
            'file.config': 'CFG',
            'technique.obfuscation': 'OB',
            'attribution.family': 'IMP',
            'attribution.implant': 'IMP',
            'technique.config': 'CFG',
            'attribution.actor': 'TA',
        }.get(tag_type, None)
        if sub_tag:
            tag_type = 'attribution'
            tag_value = "%s [%s]" % (tag_value, sub_tag)

        if tag_type == 'av.virus_name':
            if tag_value in (
                'Corrupted executable file',
                'Encrypted container deleted',
                'Encrypted container deleted;',
                'Password-protected',
                'Malformed container violation',
            ):
                tag_type = 'file.behavior'

            else:
                av_name = (tag_context or '').split('scanner:')
                if len(av_name) == 2:
                    av_name = av_name[1]
                else:
                    av_name = service_name_from_key(t['key'])

                if av_name:
                    tag_value = "[%s] %s" % (av_name, tag_value)

        summary_values = summary.get(tag_type, None)
        if summary_values is not None:
            summary_values.add(tag_value)

    return max_classification, summary


def generate_alert_id(alert_data):
    parts = [
        alert_data['submission']['params']['psid'] or alert_data['submission']['sid'],
        alert_data['submission']['time']
    ]
    return hashlib.md5("-".join(parts).encode("utf-8")).hexdigest()


# noinspection PyBroadException
def parse_submission_record(counter, datastore, alert_data, logger):
    sid = alert_data['submission']['sid']
    psid = alert_data['submission']['params']['psid']
    srecord = get_submission_record(counter, datastore, sid)

    max_classification, summary = get_summary(datastore, srecord)
    behaviors = list(summary['file.behavior'])

    extended_scan = alert_data['extended_scan']
    if psid:
        try:
            # Get errors from parent submission and submission. Strip keys
            # to only sha256 and service name. If there are any keys that
            # did not exist in the parent the extended scan is 'incomplete'.
            ps = datastore.submission.get(psid, as_obj=False) or {}
            pe = set((x[:x.rfind('.')] for x in ps.get('errors', [])))
            e = set((x[:x.rfind('.')] for x in srecord.get('errors', [])))
            ne = e.difference(pe)
            extended_scan = 'incomplete' if ne else 'completed'
        except Exception:  # pylint: disable=W0702
            logger.exception('Problem determining extended scan state:')

    domains = summary['network.domain.dynamic'].union(summary['network.domain.static'])
    ips = summary['network.ip.dynamic'].union(summary['network.ip.static'])

    return {
        'behaviors': behaviors,
        'extended_scan': extended_scan,
        'domains': domains,
        'ips': ips,
        'summary': summary,
        'srecord': srecord,
        'max_classification': max_classification
    }


AL_FIELDS = [
    'attrib',
    'av',
    'behavior',
    'domain',
    'domain_dynamic',
    'domain_static',
    'ip',
    'ip_dynamic',
    'ip_static',
    'yara'
]


def perform_alert_update(datastore, logger, alert):
    alert_id = alert.get('alert_id')

    with Lock(f"alert-update-{alert_id}", 5):
        old_alert = datastore.alert.get(alert_id, as_obj=False)
        if old_alert is None:
            raise KeyError(f"{alert_id} is missing from the alert collection.")

        # Merge fields...
        merged = {
            x: list(set(old_alert.get('al', {}).get(x, [])).union(set(alert['al'].get(x, [])))) for x in AL_FIELDS
        }

        # Sanity check.
        if not all([old_alert.get(x, None) == alert.get(x, None) for x in config.core.alerter.constant_alert_fields]):
            raise ValueError("Constant alert field changed. (%s, %s)" % (str(old_alert), str(alert)))

        old_alert = recursive_update(old_alert, alert)
        old_alert['al'] = recursive_update(old_alert['al'], merged)

        datastore.alert.save(alert_id, old_alert)

    logger.info(f"Alert {alert_id} has been updated.")


def save_alert(datastore, counter, logger, alert, psid):
    if psid:
        msg_type = "AlertUpdated"
        perform_alert_update(datastore, logger, alert)
        counter.increment('updated')
        ret_val = 'update'
    else:
        msg_type = "AlertCreated"
        datastore.alert.save(alert['alert_id'], alert)
        logger.info(f"Alert {alert['alert_id']} has been created.")
        counter.increment('created')
        ret_val = 'create'

    msg = AlertMessage({
        "msg": alert,
        "msg_type": msg_type,
        "sender": "alerter"
    })
    CommsQueue('alerts').publish(msg.as_primitives())
    return ret_val


# noinspection PyTypeChecker
def get_alert_update_parts(counter, datastore, alert_data, logger):
    global cache
    # Check cache
    alert_file = alert_data['submission']['files'][0]
    alert_update_p1, alert_update_p2 = cache.get(alert_data['submission']['sid'], (None, None))
    if alert_update_p1 is None or alert_update_p2 is None:
        parsed_record = parse_submission_record(counter, datastore, alert_data, logger)
        file_record = datastore.file.get(alert_file['sha256'], as_obj=False)
        alert_update_p1 = {
            'extended_scan': parsed_record['extended_scan'],
            'al': {
                'attrib': list(parsed_record['summary']['attribution']),
                'av': list(parsed_record['summary']['av.virus_name']),
                'behavior': parsed_record['behaviors'],
                'domain': list(parsed_record['domains']),
                'domain_dynamic': list(parsed_record['summary']['network.domain.dynamic']),
                'domain_static': list(parsed_record['summary']['network.domain.static']),
                'ip': list(parsed_record['ips']),
                'ip_dynamic': list(parsed_record['summary']['network.ip.dynamic']),
                'ip_static': list(parsed_record['summary']['network.ip.static']),
                'request_end_time': parsed_record['srecord']['times']['completed'],
                'yara': list(parsed_record['summary']['file.rule.yara']),
            },

        }
        alert_update_p2 = {
            'classification': parsed_record['max_classification'],
            'file': {
                'md5': file_record['md5'],
                'sha1': file_record['sha1'],
                'sha256': file_record['sha256'],
                'size': file_record['size'],
                'type': file_record['type']
            }
        }
        cache.add(alert_data['submission']['sid'], (alert_update_p1, alert_update_p2))

    alert_update_p1['reporting_ts'] = now_as_iso()
    alert_update_p1['file'] = {'name': alert_file['name']}

    return alert_update_p1, alert_update_p2


def process_alert_message(counter, datastore, logger, alert_data):
    """
    This is the default process_alert_message function. If the generic alerts are not sufficient
    in your deployment, you can create another method like this one that would follow
    the same structure but with added parts where the comment blocks are located.
    """

    ###############################
    # Additional init goes here
    ###############################

    alert = {
        'al': {
            'score': alert_data['score']
        },
        'alert_id': generate_alert_id(alert_data),
        'expiry_ts': now_as_iso(config.core.alerter.alert_ttl * 24 * 60 * 60),
        'metadata': {safe_str(key): value for key, value in alert_data['submission']['metadata'].items()},
        'sid': alert_data['submission']['sid'],
        'ts': alert_data['submission']['time'],
        'type': alert_data['submission']['params']['type']
    }

    ###############################
    # Additional alert_data parsing
    # and alert updating goes here
    ###############################

    # Get update parts
    alert_update_p1, alert_update_p2 = get_alert_update_parts(counter, datastore, alert_data, logger)

    # Update alert with default values
    alert = recursive_update(alert, alert_update_p1)

    # Update alert with computed values
    alert = recursive_update(alert, alert_update_p2)

    return save_alert(datastore, counter, logger, alert, alert_data['submission']['params']['psid'])
