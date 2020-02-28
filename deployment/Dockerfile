FROM cccs/assemblyline:latest

ARG version

# Switch to root to install dependancies
USER root

RUN pip install assemblyline_core==$version

# clear pip cache
RUN rm -rf ~/.cache/pip

# Switch back to assemblyline user
USER assemblyline
