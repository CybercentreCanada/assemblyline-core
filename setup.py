
import os

from setuptools import setup, find_packages

# For development and local builds use this version number, but for real builds replace it
# with the tag found in the environment
package_version = "4.0.0.dev0"
if 'BITBUCKET_TAG' in os.environ:
    package_version = os.environ['BITBUCKET_TAG'].lstrip('v')
elif 'BUILD_SOURCEBRANCH' in os.environ:
    full_tag_prefix = 'refs/tags/v'
    package_version = os.environ['BUILD_SOURCEBRANCH'][len(full_tag_prefix):]


setup(
    name="assemblyline-core",
    version=package_version,
    description="Assemblyline (v4) automated malware analysis framework - Core components.",
    long_description="This package provides the core components of Assemblyline v4 malware analysis framework. "
                     "(Alerter, Dispatcher, Expiry, Ingester, Metrics, Watcher, Workflow)",
    url="https://github.com/CybercentreCanada/assemblyline-core/",
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
    extras_require={
        'test': [
            'fakeredis[lua]',
            'pytest'
        ]
    },
    tests_require=[
        'pytest',
    ],
    package_data={
        '': ["*classification.yml", "*.magic"]
    }
)
