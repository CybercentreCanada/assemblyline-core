
from assemblyline_core.metrics.metrics_server import MetricsServer
from assemblyline.common import forge

if __name__ == '__main__':
    config = forge.get_config()
    with MetricsServer(config=config) as metricsd:
        metricsd.serve_forever()
