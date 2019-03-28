
from al_core.metrics.hashmap_metrics import HashMapMetricsServer
from al_core.metrics.legacy_metrics import LegacyMetricsServer
from assemblyline.common import forge, metrics

if __name__ == '__main__':
    config = forge.get_config()
    if config.core.metrics.type == metrics.LEGACY:
        with LegacyMetricsServer(config=config) as metricsd:
            metricsd.serve_forever()
    else:
        with HashMapMetricsServer(config=config) as metricsd:
            metricsd.serve_forever()
