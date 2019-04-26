
from setuptools import setup, find_packages


setup(
    name="assemblyline-core",
    version="4.0.0.dev6",
    description="Assemblyline (v4) automated malware analysis framework - Core components.",
    long_description="This package provides the core components of Assemblyline v4 malware analysis framework. "
                     "(Alerter, Dispatcher, Expiry, Ingester, Metrics, Watcher, Workflow)",
    url="https://bitbucket.org/cse-assemblyline/alv4_core/",
    author="CCCS Assemblyline development team",
    author_email="assemblyline@cyber.gc.ca",
    license="MIT",
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords="assemblyline malware gc canada cse-cst cse cst cyber cccs",
    packages=find_packages(),
    install_requires=[
        'urllib3<1.25',
        'python-baseconv',
        'assemblyline',
        'elastic-apm[flask]'
    ],
    package_data={
        '': ["*schema.xml", "*managed-schema", "*solrconfig.xml", "*classification.yml", "*.magic"]
    }
)
