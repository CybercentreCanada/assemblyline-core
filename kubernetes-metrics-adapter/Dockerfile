FROM busybox:latest AS builder
RUN mkdir -m 777 /working

FROM scratch
COPY assemblyline-kubernetes /assemblyline-kubernetes

COPY --from=builder --chown=1000:1000 --chmod=777 /working /working
WORKDIR /working
USER 1000

ENTRYPOINT ["/assemblyline-kubernetes"]