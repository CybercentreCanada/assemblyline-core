FROM cccs/assemblyline:latest
ARG version

RUN pip install --no-cache-dir --user assemblyline_core==$version && rm -rf ~/.cache/pip
