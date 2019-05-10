FROM python:3.7

ARG version

RUN mkdir -p /etc/assemblyline
RUN mkdir -p /var/log/assemblyline
RUN mkdir -p /var/cache/assemblyline

RUN apt-get update && apt-get install -yy build-essential libffi-dev libfuzzy-dev
RUN pip install assemblyline_core==$version
