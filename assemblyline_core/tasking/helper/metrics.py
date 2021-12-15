from assemblyline.common.metrics import MetricsFactory
from assemblyline.odm.messages.service_heartbeat import Metrics
from assemblyline_core.tasking.config import LOCK

METRICS_FACTORIES = {}


def get_metrics_factory(service_name):
    factory = METRICS_FACTORIES.get(service_name, None)

    if factory is None:
        with LOCK:
            factory = MetricsFactory('service', Metrics, name=service_name, export_zero=False)
            METRICS_FACTORIES[service_name] = factory

    return factory
