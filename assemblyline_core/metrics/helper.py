import json
import time

import elasticsearch

from assemblyline.datastore.exceptions import ILMException
from packaging import version

MAX_RETRY_BACKOFF = 10


def ilm_policy_exists(es, name):
    conn = es.transport.get_connection()
    pol_req = conn.session.get(f"{conn.base_url}/_ilm/policy/{name}")
    if pol_req.status_code in [400, 401, 403, 500, 501, 503, 504]:
        raise ILMException(f"[{pol_req.status_code}] {pol_req.reason}")
    return pol_req.ok


def create_ilm_policy(es, name, ilm_config):
    data_base = {
        "policy": {
            "phases": {
                "hot": {
                    "min_age": "0ms",
                    "actions": {
                        "set_priority": {
                            "priority": 100
                        },
                        "rollover": {
                            "max_age": f"{ilm_config['warm']}{ilm_config['unit']}"
                        }
                    }
                },
                "warm": {
                    "actions": {
                        "set_priority": {
                            "priority": 50
                        }
                    }
                },
                "cold": {
                    "min_age": f"{ilm_config['cold']}{ilm_config['unit']}",
                    "actions": {
                        "set_priority": {
                            "priority": 20
                        }
                    }
                }
            }
        }
    }

    if ilm_config['delete']:
        data_base['policy']['phases']['delete'] = {
            "min_age": f"{ilm_config['delete']}{ilm_config['unit']}",
            "actions": {
                "delete": {}
            }
        }

    conn = es.transport.get_connection()
    pol_req = conn.session.put(f"{conn.base_url}/_ilm/policy/{name}",
                               headers={"Content-Type": "application/json"}, data=json.dumps(data_base))
    if not pol_req.ok:
        raise ILMException(f"ERROR: Failed to create ILM policy: {name}")


def ensure_indexes(log, es, config, indexes, datastream_enabled=False):
    for index_type in indexes:
        try:
            index = f"al_metrics_{index_type}_ds" if datastream_enabled else f"al_metrics_{index_type}"
            policy = f"{index}_policy"
            while True:
                try:
                    while not ilm_policy_exists(es, policy):
                        log.debug(f"ILM Policy {policy.upper()} does not exists. Creating it now...")
                        create_ilm_policy(es, policy, config.as_primitives())
                    break
                except ILMException as e:
                    log.warning(str(e))
                    time.sleep(1)
                    pass

            if not with_retries(log, es.indices.exists_template, index):
                log.debug(f"Index template {index.upper()} does not exists. Creating it now...")

                template_body = {
                    "settings": {
                        "index.lifecycle.name": policy,
                        "index.codec": "best_compression"
                    }
                }
                es_version = version.parse(es.info()['version']['number'])
                # Check support for component templates (>=7.8)
                if es_version >= version.parse("7.8"):
                    component_name = f"{index}-settings"
                    component_body = {"template": template_body}
                    if not es.cluster.exists_component_template(component_name):
                        try:
                            # Create component template
                            with_retries(log, es.cluster.put_component_template, component_name, component_body)
                        except elasticsearch.exceptions.RequestError as e:
                            if "resource_already_exists_exception" not in str(e):
                                raise
                            log.warning(f"Tried to create a component template that already exists: {index.upper()}")
                    template_body = {"index_patterns": [f"{index}-*"], "composed_of": [component_name]}
                    if datastream_enabled:
                        template_body['index_patterns'] = f"{index}*"
                        template_body['data_stream'] = {}
                        template_body['priority'] = 10

                # Legacy template
                else:
                    template_body["order"] = 1
                    template_body["index_patterns"] = [f"{index}-*"]
                    template_body["settings"]["index.lifecycle.rollover_alias"] = index

                try:
                    with_retries(log, es.indices.put_index_template, index, template_body)
                except elasticsearch.exceptions.RequestError as e:
                    if "resource_already_exists_exception" not in str(e):
                        raise
                    log.warning(f"Tried to create an index template that already exists: {index.upper()}")

            if not with_retries(log, es.indices.exists_alias, index) and not datastream_enabled:
                log.debug(f"Index alias {index.upper()} does not exists. Creating it now...")

                index_body = {"aliases": {index: {"is_write_index": True}}}

                while True:
                    try:
                        with_retries(log, es.indices.create, f"{index}-000001", index_body)
                        break
                    except elasticsearch.exceptions.RequestError as e:
                        if "resource_already_exists_exception" in str(e):
                            log.warning(f"Tried to create an index template that "
                                        f"already exists: {index.upper()}-000001")
                            break
                        elif "invalid_alias_name_exception" in str(e):
                            with_retries(log, es.indices.delete, index)
                            log.warning(str(e))
                            time.sleep(1)
                        else:
                            raise

        except Exception as e:
            log.exception(e)


def with_retries(log, func, *args, **kwargs):
    retries = 0
    updated = 0
    deleted = 0
    while True:
        try:
            ret_val = func(*args, **kwargs)

            if retries:
                log.info('Reconnected to elasticsearch!')

            if updated:
                ret_val['updated'] += updated

            if deleted:
                ret_val['deleted'] += deleted

            return ret_val

        except elasticsearch.exceptions.NotFoundError:
            raise

        except elasticsearch.exceptions.ConflictError as ce:
            updated += ce.info.get('updated', 0)
            deleted += ce.info.get('deleted', 0)

            time.sleep(min(retries, MAX_RETRY_BACKOFF))
            retries += 1

        except (elasticsearch.exceptions.ConnectionError,
                elasticsearch.exceptions.ConnectionTimeout,
                elasticsearch.exceptions.AuthenticationException):
            log.warning(f"No connection to Elasticsearch, retrying...")
            time.sleep(min(retries, MAX_RETRY_BACKOFF))
            retries += 1

        except elasticsearch.exceptions.TransportError as e:
            err_code, msg, cause = e.args
            if err_code == 503 or err_code == '503':
                log.warning("Looks like index is not ready yet, retrying...")
                time.sleep(min(retries, MAX_RETRY_BACKOFF))
                retries += 1
            elif err_code == 429 or err_code == '429':
                log.warning("Elasticsearch is too busy to perform the requested task, "
                            "we will wait a bit and retry...")
                time.sleep(min(retries, MAX_RETRY_BACKOFF))
                retries += 1

            else:
                raise
