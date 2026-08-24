
from assemblyline_core.scaler_hpa.scaler_server import ScalerServer


if __name__ == '__main__':
    with ScalerServer() as scaler:
        scaler.serve_forever()
