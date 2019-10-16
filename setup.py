
import os

from setuptools import setup, find_packages

# For development and local builds use this version number, but for real builds replace it
# with the tag found in the environment
package_version = "4.0.0.dev0"
for variable_name in ['BITBUCKET_TAG']:
    package_version = os.environ.get(variable_name, package_version)
    package_version = package_version.lstrip('v')


setup(
    name="assemblyline-core",
    version=package_version,
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
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords="assemblyline malware gc canada cse-cst cse cst cyber cccs",
    packages=find_packages(exclude=['deployment/*', 'test/*']),
    install_requires=[
        'urllib3<1.25',
        'assemblyline',
        'docker',
    ],
    tests_require=[
        'pytest',
    ],
    package_data={
        '': ["*schema.xml", "*managed-schema", "*solrconfig.xml", "*classification.yml", "*.magic"]
    }
)
