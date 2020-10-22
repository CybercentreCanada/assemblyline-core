ARG branch=latest
FROM cccs/assemblyline:$branch
ARG version

RUN pip install --no-cache-dir --user assemblyline-core==$version && rm -rf ~/.cache/pip
