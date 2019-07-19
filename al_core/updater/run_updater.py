from al_core.server_base import ServerBase


class ServiceUpdater(ServerBase):
    def __init__(self, logger=None):
        super().__init__('assemblyline.service.updater', logger=logger)


if __name__ == '__main__':
    ServiceUpdater().serve_forever()
