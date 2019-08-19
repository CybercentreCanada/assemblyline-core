
from assemblyline_core.metrics.metrics_server import HeartbeatManager
from assemblyline.common import forge

if __name__ == '__main__':
    config = forge.get_config()
    with HeartbeatManager(config=config) as metricsd:
        metricsd.serve_forever()
