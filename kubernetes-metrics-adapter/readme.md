
# Assemblyline to Kubernetes metrics adapter

Feeds metrics about assemblyline processing to kubernetes to allow native service scaling.

## Build

```bash
CGO_ENABLED=0 go build
docker build . -t cccs/assemblyline-k8s-metrics-adapter:latest
```