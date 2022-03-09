# Replay instructions

## Create a replay container

```shell
docker build --no-cache -t cccs/assemblyline-replay .
```

## Creator

Creator runs periodically a set or queries to generate bundle files to be imported into another server

### Configure creator

Create a replay.yml file with the following content

```yaml
creator:
  client:
    options:
      host: "https://<YOUR SOURCE SERVER DOMAIN/IP>:443",
      apikey: "<PASTE YOUR SOURCE API KEY>"
      user: <YOUR SOURCE USERNAME>
      verify: false
```

### Run creator

```shell
docker run --rm \
  --name assemblyline-replay-creator \
  -v /etc/assemblyline/classification.yml:/etc/assemblyline/classification.yml:ro \
  -v /etc/assemblyline/config.yml:/etc/assemblyline/config.yml:ro \
  -v /etc/assemblyline/replay_creator.yml:/etc/assemblyline/replay.yml:ro \
  -v /tmp/replay/files:/tmp/replay/output \
  -v /tmp/replay/creator:/tmp/replay/work \
  cccs/assemblyline-replay \
  python -m assemblyline_core.replay.run_replay_creator
```

## Loader

Loader loads bundles from disk and import them into another server

### Configure Loader

Create a replay.yml file with the following content

```yaml
loader:
  client:
    options:
      host: "https://<YOUR SOURCE SERVER DOMAIN/IP>:443",
      apikey: "<PASTE YOUR SOURCE API KEY>"
      user: <YOUR SOURCE USERNAME>
      verify: false
```

### Run Loader

```shell
docker run --rm \
  --name assemblyline-replay-loader \
  -v /etc/assemblyline/classification.yml:/etc/assemblyline/classification.yml:ro \
  -v /etc/assemblyline/config.yml:/etc/assemblyline/config.yml:ro \
  -v /etc/assemblyline/replay_loader.yml:/etc/assemblyline/replay.yml:ro \
  -v /tmp/replay/files:/tmp/replay/input \
  -v /tmp/replay/failed:/tmp/replay/failed \
  -v /tmp/replay/loader:/tmp/replay/work \
  cccs/assemblyline-replay \
  python -m assemblyline_core.replay.run_replay_loader
```
