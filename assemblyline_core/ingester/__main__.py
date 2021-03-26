from assemblyline_core.ingester.ingester import Ingester


with Ingester() as server:
    server.serve_forever()
