ARG branch=latest
FROM cccs/assemblyline:$branch
ARG version

HEALTHCHECK CMD find /tmp/heartbeat -newermt -30 seconds

RUN pip install --no-cache-dir --user assemblyline-core==$version && rm -rf ~/.cache/pip
