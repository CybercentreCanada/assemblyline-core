from assemblyline.common import forge
from assemblyline.common.caching import TimeExpiredCache
from assemblyline.common.dict_utils import recursive_update
from assemblyline.common.str_utils import safe_str
from assemblyline.common.isotime import now_as_iso
from assemblyline.datastore.exceptions import VersionConflictException
from assemblyline.odm.messages.alert import AlertMessage
from assemblyline.remote.datatypes.queues.comms import CommsQueue

CACHE_LEN = 60 * 60 * 24
CACHE_EXPIRY_RATE = 60

action_queue_map = None
cache = TimeExpiredCache(CACHE_LEN, CACHE_EXPIRY_RATE)
Classification = forge.get_classification()
config = forge.get_config()


SUMMARY_TYPE_MAP = {
    'av.virus_name': 'av',
    'attribution.exploit': 'attrib',
    'attribution': 'attrib',
    'file.config': 'attrib',
    'technique.obfuscation': 'attrib',
    'file.behavior': 'behavior',
    'file.rule.yara': 'yara',
    'attribution.family': 'attrib',
    'attribution.implant': 'attrib',
    'network.static.domain': 'domain_static',
    'network.dynamic.domain': 'domain_dynamic',
    'network.static.ip': 'ip_static',
    'network.dynamic.ip': 'ip_dynamic',
    'network.static.uri': 'uri_static',
    'network.dynamic.uri': 'uri_dynamic',
    'technique.config': 'attrib',
    'attribution.actor': 'attrib',
    'heuristic.name': 'heuristic',
    'attack.pattern': 'attack_pattern',
    'attack.category': 'attack_category'
}

AL_RESULT_KEYS = [
    'attrib',
    'av',
    'behavior',
    'domain',
    'domain_dynamic',
    'domain_static',
    'ip',
    'ip_dynamic',
    'ip_static',
    'uri',
    'uri_dynamic',
    'uri_static',
    'yara']

VERDICT_RANK_MAP = {
    "safe": 0,
    "info": 1,
    "suspicious": 2,
    "malicious": 3
}

TAG_MAP = {
    'attribution.exploit': 'EXP',
    'file.config': 'CFG',
    'technique.obfuscation': 'OB',
    'attribution.family': 'IMP',
    'attribution.implant': 'IMP',
    'technique.config': 'CFG',
    'attribution.actor': 'TA',
}

AV_TO_BEAVIOR = (
    'Corrupted executable file',
    'Encrypted container deleted',
    'Encrypted container deleted;',
    'Password-protected',
    'Malformed container violation',
)


class AlertMissingError(Exception):
    pass


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


def get_summary(datastore, srecord, user_classification):
    max_classification = srecord['classification']

    detailed = {v: {} for v in SUMMARY_TYPE_MAP.values()}

    submission_summary = datastore.get_summary_from_keys(srecord.get('results', []), cl_engine=Classification,
                                                         user_classification=user_classification)
    max_classification = Classification.max_classification(max_classification, submission_summary['classification'])

    # Process Att&cks
    for attack in submission_summary['attack_matrix']:
        tag_type = 'attack_pattern'
        item_key = attack['name']
        if item_key not in detailed[tag_type]:
            detailed[tag_type][item_key] = {
                "type": tag_type,
                "subtype": None,
                "verdict": attack['h_type'],
                "value": item_key
            }
        else:
            if VERDICT_RANK_MAP[detailed[tag_type][item_key]['verdict']] < VERDICT_RANK_MAP[attack['h_type']]:
                detailed[tag_type][item_key]['verdict'] = attack['h_type']

        # Process Att&cks categories
        for cat in attack['categories']:
            tag_type = 'attack_category'
            if cat not in detailed[tag_type]:
                detailed[tag_type][cat] = {
                    "type": tag_type,
                    "subtype": None,
                    "verdict": attack['h_type'],
                    "value": cat
                }
            else:
                if VERDICT_RANK_MAP[detailed[tag_type][cat]['verdict']] < VERDICT_RANK_MAP[attack['h_type']]:
                    detailed[tag_type][cat]['verdict'] = attack['h_type']

    # Process Heuristics
    for h_type, heur_list in submission_summary['heuristics'].items():
        for heur in heur_list:
            tag_type = 'heuristic'
            item_key = heur['name']
            if item_key not in detailed[tag_type]:
                detailed[tag_type][item_key] = {
                    "type": tag_type,
                    "subtype": None,
                    "verdict": h_type,
                    "value": item_key
                }
            else:
                if VERDICT_RANK_MAP[detailed[tag_type][item_key]['verdict']] < VERDICT_RANK_MAP[h_type]:
                    detailed[tag_type][item_key]['verdict'] = h_type

    # Process Tags
    for t in submission_summary['tags']:
        if t.get('safelisted', False):
            continue

        tag_value = t['value']
        tag_type = SUMMARY_TYPE_MAP.get(t['type'], None)

        if tag_value == '' or tag_type is None:
            continue

        sub_type = TAG_MAP.get(t['type'], None)

        if tag_type == 'av':
            if tag_value in AV_TO_BEAVIOR:
                tag_type = 'behavior'

        item_key = f"{t['type']}__{t['value']}"
        if item_key not in detailed[tag_type]:
            detailed[tag_type][item_key] = {
                "type": t['type'],
                "subtype": sub_type,
                "verdict": t['h_type'],
                "value": t['value']
            }
        else:
            if VERDICT_RANK_MAP[detailed[tag_type][item_key]['verdict']] < VERDICT_RANK_MAP[t['h_type']]:
                detailed[tag_type][item_key]['verdict'] = t['h_type']

    # Simplify the detailed items
    detailed = {k: list(v.values()) for k, v in detailed.items()}

    # Generate the Summary
    summary = {k: set([f"{item['value']} [{item['subtype']}]" if item.get('subtype', None) else item['value']
                       for item in v]) for k, v in detailed.items()}
    summary['domain'] = summary['domain_dynamic'].union(summary['domain_static'])
    summary['ip'] = summary['ip_dynamic'].union(summary['ip_static'])
    summary['uri'] = summary['uri_dynamic'].union(summary['uri_static'])

    # Merge IPs and domains in details
    domains = list(detailed.pop('domain_dynamic', {}))
    domains.extend(detailed.pop('domain_static', {}))
    ips = list(detailed.pop('ip_dynamic', {}))
    ips.extend(detailed.pop('ip_static', {}))
    uris = list(detailed.pop('uri_dynamic', {}))
    uris.extend(detailed.pop('uri_static', {}))
    detailed['ip'] = ips
    detailed['domain'] = domains
    detailed['uri'] = uris

    return max_classification, summary, submission_summary['filtered'], detailed


def generate_alert_id(logger, alert_data):
    if alert_data['ingest_id']:
        return alert_data['ingest_id']
    sid = alert_data['submission']['params']['psid'] or alert_data['submission']['sid']
    logger.info(f"ingest_id not found for sid={sid}")
    return sid


# noinspection PyBroadException
def parse_submission_record(counter, datastore, alert_data, logger, user_classification):
    sid = alert_data['submission']['sid']
    psid = alert_data['submission']['params']['psid']
    srecord = get_submission_record(counter, datastore, sid)

    max_classification, summary, filtered, detailed = get_summary(datastore, srecord, user_classification)

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

    return {
        'extended_scan': extended_scan,
        'summary': summary,
        'srecord': srecord,
        'max_classification': max_classification,
        'filtered': filtered,
        'detailed': detailed
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
    alert_id = alert.get('alert_id', None)
    if not alert_id:
        raise ValueError(f"We could not find the alert ID in the alert: {str(alert)}")

    while True:
        old_alert, version = datastore.alert.get_if_exists(
            alert_id, as_obj=False, archive_access=config.datastore.ilm.update_archive, version=True)
        if old_alert is None:
            raise AlertMissingError(f"{alert_id} is missing from the alert collection.")

        # Ensure alert keeps original timestamp
        alert['ts'] = old_alert['ts']

        # Merge fields...
        merged = {
            x: list(set(old_alert.get('al', {}).get(x, [])).union(set(alert['al'].get(x, [])))) for x in AL_FIELDS
        }

        # Sanity check.
        if not all([old_alert.get(x, None) == alert.get(x, None) for x in config.core.alerter.constant_alert_fields]):
            raise ValueError(f"Constant alert field changed. ({str(old_alert)}, {str(alert)})")

        old_alert = recursive_update(old_alert, alert)
        old_alert['al'] = recursive_update(old_alert['al'], merged)
        old_alert['workflows_completed'] = False

        try:
            datastore.alert.save(alert_id, old_alert, version=version)
            logger.info(f"Alert {alert_id} has been updated.")
            return
        except VersionConflictException as vce:
            logger.info(f"Retrying update alert due to version conflict: {str(vce)}")


def save_alert(datastore, counter, logger, alert, psid):
    def create_alert():
        msg_type = "AlertCreated"
        datastore.alert.save(alert['alert_id'], alert)
        logger.info(f"Alert {alert['alert_id']} has been created.")
        counter.increment('created')
        ret_val = 'create'
        return msg_type, ret_val

    if psid:
        try:
            msg_type = "AlertUpdated"
            perform_alert_update(datastore, logger, alert)
            counter.increment('updated')
            ret_val = 'update'
        except AlertMissingError as e:
            logger.info(f"{str(e)}. Creating a new alert [{alert['alert_id']}]...")
            msg_type, ret_val = create_alert()
    else:
        msg_type, ret_val = create_alert()

    msg = AlertMessage({
        "msg": alert,
        "msg_type": msg_type,
        "sender": "alerter"
    })
    CommsQueue('alerts').publish(msg.as_primitives())
    return ret_val


# noinspection PyTypeChecker
def get_alert_update_parts(counter, datastore, alert_data, logger, user_classification):
    global cache
    # Check cache
    alert_file = alert_data['submission']['files'][0]
    alert_update_p1, alert_update_p2 = cache.get(alert_data['submission']['sid'], (None, None))
    if alert_update_p1 is None or alert_update_p2 is None:
        parsed_record = parse_submission_record(counter, datastore, alert_data, logger, user_classification)
        file_record = datastore.file.get(alert_file['sha256'], as_obj=False)
        al = {k: list(v) for k, v in parsed_record['summary'].items() if k in AL_RESULT_KEYS}
        al['detailed'] = parsed_record['detailed']
        al['request_end_time'] = parsed_record['srecord']['times']['completed']
        alert_update_p1 = {
            'extended_scan': parsed_record['extended_scan'],
            'filtered': parsed_record['filtered'],
            'al': al,
            'attack': {
                'pattern': list(parsed_record['summary']['attack_pattern']),
                'category': list(parsed_record['summary']['attack_category'])

            },
            'heuristic': {
                'name': list(parsed_record['summary']['heuristic'])
            }
        }
        alert_update_p2 = {
            'classification': parsed_record['max_classification'],
            'file': {
                'md5': file_record['md5'],
                'sha1': file_record['sha1'],
                'sha256': file_record['sha256'],
                'size': file_record['size'],
                'type': file_record['type']
            },
            'verdict': {
                "malicious": parsed_record['srecord']['verdict']['malicious'],
                "non_malicious": parsed_record['srecord']['verdict']['non_malicious']
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

    user = datastore.user.get(alert_data.get('submission', {}).get('params', {}).get('submitter', None), as_obj=False)
    if user:
        user_classification = user['classification']
    else:
        user_classification = None
    a_type = alert_data.get('submission', {}).get('metadata', {}).pop('type', None)
    a_ts = alert_data.get('submission', {}).get('metadata', {}).pop('ts', None)

    alert = {
        'al': {
            'score': alert_data['score']
        },
        'alert_id': generate_alert_id(logger, alert_data),
        'archive_ts': now_as_iso(config.datastore.ilm.days_until_archive * 24 * 60 * 60),
        'metadata': {safe_str(key): value for key, value in alert_data['submission']['metadata'].items()},
        'sid': alert_data['submission']['sid'],
        'ts': a_ts or alert_data['submission']['time'],
        'type': a_type or alert_data['submission']['params']['type']
    }

    if config.core.alerter.alert_ttl:
        alert['expiry_ts'] = now_as_iso(config.core.alerter.alert_ttl * 24 * 60 * 60)

    ###############################
    # Additional alert_data parsing
    # and alert updating goes here
    ###############################

    # Get update parts
    alert_update_p1, alert_update_p2 = get_alert_update_parts(counter, datastore,
                                                              alert_data, logger,
                                                              user_classification)

    # Update alert with default values
    alert = recursive_update(alert, alert_update_p1)

    # Update alert with computed values
    alert = recursive_update(alert, alert_update_p2)

    return save_alert(datastore, counter, logger, alert, alert_data['submission']['params']['psid'])
