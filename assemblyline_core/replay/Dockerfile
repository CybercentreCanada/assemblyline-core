ARG branch=stable
ARG base=cccs/assemblyline
FROM cccs/assemblyline-core:$branch

ARG branch
RUN [ "$branch" = "latest" ] && \
    pip install --no-cache-dir --user --pre assemblyline_client || \
    pip install --no-cache-dir --user assemblyline_client
