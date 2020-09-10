ARG branch=latest
FROM cccs/assemblyline:$branch
ARG version

HEALTHCHECK CMD bash -c "if [[ ! `find /tmp/heartbeat -newermt '-30 seconds'` ]]; then false; fi"

RUN pip install --no-cache-dir --user assemblyline-core==$version && rm -rf ~/.cache/pip
