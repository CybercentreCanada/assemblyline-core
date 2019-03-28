
from al_core.metrics.hashmap_metrics import HashMapHeartbeatManager
from al_core.metrics.legacy_metrics import LegacyHeartbeatManager
from assemblyline.common import forge, metrics

if __name__ == '__main__':
    config = forge.get_config()
    if config.core.metrics.type == metrics.LEGACY:
        with LegacyHeartbeatManager(config=config) as metricsd:
            metricsd.serve_forever()
    else:
        with HashMapHeartbeatManager(config=config) as metricsd:
            metricsd.serve_forever()
