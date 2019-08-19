
from assemblyline_core.scaler.scaler_server import ScalerServer


if __name__ == '__main__':
    with ScalerServer() as scaler:
        scaler.serve_forever()
