from cryptography.x509 import CertificateBuilder, random_serial_number, BasicConstraints, Name, NameAttribute, NameOID, load_pem_x509_certificate
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives._serialization import Encoding, PrivateFormat, NoEncryption
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.primitives.asymmetric import rsa
from datetime import datetime, timedelta
from typing import Tuple

import os

CERT_SERVER_HOST = os.environ.get('CERT_SERVER_HOST', 'cert_server')
CERT_SERVER_TOKEN = os.environ.get('CERT_SERVER_TOKEN', 'ChangeMe')

AL_CERT_DIR = os.environ.get('AL_CERT_DIR', '/etc/assemblyline/ssl/')
AL_ROOT_CA_CERT = os.environ.get('AL_ROOT_CA_CERT', os.path.join(AL_CERT_DIR, 'al_root-ca.crt'))
AL_ROOT_CA_KEY = os.environ.get('AL_ROOT_CA_KEY', os.path.join(AL_CERT_DIR, 'al_root-ca.key'))
AL_ROOT_CA_PASSWORD = os.environ.get('AL_ROOT_CA_PASSWORD')


def generate_root_ca():
    # Create key pair for Root CA
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()

    # Generate Root CA
    CA_CN = Name([NameAttribute(NameOID.COMMON_NAME, 'assemblyline')])
    builder = CertificateBuilder(subject_name=CA_CN, issuer_name=CA_CN, public_key=public_key,
                                 serial_number=random_serial_number(),
                                 not_valid_before=datetime.now(),
                                 not_valid_after=datetime.now() + timedelta(days=365))
    builder = builder.add_extension(BasicConstraints(ca=True, path_length=None), critical=True)
    root_cert = builder.sign(private_key=private_key, algorithm=hashes.SHA256())

    # Store cert, private_key for signing server certs.
    # TODO: Docker shared volume mounts?
    # TODO: Kubernetes secrets to be used by Scaler/Updater
    open(AL_ROOT_CA_CERT, 'wb').write(root_cert.public_bytes(encoding=Encoding.PEM))
    open(AL_ROOT_CA_KEY, 'wb').write(private_key.private_bytes(encoding=Encoding.PEM, format=PrivateFormat.PKCS8,
                                                               encryption_algorithm=NoEncryption()))

    # with open('py_rootCA.key', 'wb') as root_key:
    #     root_key.write(private_key.private_bytes(encoding=Encoding.PEM,
    #                    format=PrivateFormat.PKCS8, encryption_algorithm=NoEncryption()))

    # with open('py_rootCA.crt', 'wb') as root_crt:
    #     root_crt.write(root_cert.public_bytes(encoding=Encoding.PEM))


def generate_server_cert(server: str, root_ca: bytes = None, root_key: bytes = None):
    # Default to on-disk root bundle
    if not root_ca:
        root_ca = open(AL_ROOT_CA_CERT, 'rb').read()

    if not root_key:
        root_key = open(AL_ROOT_CA_KEY, 'rb').read()

    root_ca = load_pem_x509_certificate(root_ca)
    root_key = load_pem_private_key(root_key, AL_ROOT_CA_PASSWORD)
    server_cn = Name([NameAttribute(NameOID.COMMON_NAME, server)])
    builder = CertificateBuilder(subject_name=server_cn, issuer_name=root_ca.subject, public_key=root_ca.public_key(),
                                 serial_number=random_serial_number(), not_valid_before=datetime.now(),
                                 not_valid_after=datetime.now() + timedelta(days=30))
    server_cert = builder.sign(private_key=root_key, algorithm=hashes.SHA256())

    # TODO Store server cert, key on disk
    open(os.path.join(AL_CERT_DIR, f"{server}.crt"), 'wb').write(server_cert.public_bytes(encoding=Encoding.PEM))
    open(os.path.join(AL_CERT_DIR, f"{server}.key"), 'wb').write(root_key.private_bytes(
        encoding=Encoding.PEM, format=PrivateFormat.PKCS8, encryption_algorithm=NoEncryption()))


def get_root_ca_bundle() -> Tuple[str, str]:
    if not os.path.exists(AL_ROOT_CA_CERT):
        generate_root_ca()
    return AL_ROOT_CA_CERT, AL_ROOT_CA_KEY


def get_server_bundle(server: str) -> Tuple[str, str]:
    if not os.path.exists(os.path.join(AL_CERT_DIR, f"{server}.crt")):
        generate_server_cert(server=server)

    # Return certificate and private key paths for the requested server
    return os.path.join(AL_CERT_DIR, f"{server}.crt"), os.path.join(AL_CERT_DIR, f"{server}.key")


# Instantiate the certificate directory, if necessary
os.makedirs(AL_CERT_DIR, exist_ok=True)

# Initialize Root CA bundle (if necessary)
get_root_ca_bundle()
