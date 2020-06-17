
from assemblyline_core.metrics.metrics_server import StatisticsAggregator
from assemblyline.common import forge

if __name__ == '__main__':
    config = forge.get_config()
    with StatisticsAggregator(config=config) as sigAggregator:
        sigAggregator.serve_forever()
