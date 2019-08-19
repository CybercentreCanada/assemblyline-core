from assemblyline_core.watcher.server import WatcherServer

if __name__ == "__main__":
    with WatcherServer() as watch:
        watch.serve_forever()
