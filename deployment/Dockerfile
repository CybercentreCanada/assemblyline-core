FROM cccs/assemblyline:latest

ARG version

# Switch to root to install dependancies
USER root
RUN pip install assemblyline_core==$version
USER assemblyline
