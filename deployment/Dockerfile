ARG branch=latest
ARG base=cccs/assemblyline
FROM $base:$branch
ARG version

# Install assemblyline base (setup.py is just a file we know exists so the command
# won't fail if dist isn't there. The dist* copies in any dist directory only if it exists.)
COPY setup.py dist* dist/
RUN pip install --no-cache-dir -f dist/ --user assemblyline-core==$version && rm -rf ~/.cache/pip
