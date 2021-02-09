from assemblyline_core.dispatching.dispatcher import Dispatcher


with Dispatcher() as server:
    server.serve_forever()
