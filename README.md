[![Discord](https://img.shields.io/badge/chat-on%20discord-7289da.svg?sanitize=true)](https://discord.gg/GUAy9wErNu)
[![](https://img.shields.io/discord/908084610158714900)](https://discord.gg/GUAy9wErNu)
[![Static Badge](https://img.shields.io/badge/github-assemblyline-blue?logo=github)](https://github.com/CybercentreCanada/assemblyline)
[![Static Badge](https://img.shields.io/badge/github-assemblyline--core-blue?logo=github)](https://github.com/CybercentreCanada/assemblyline-core)
[![GitHub Issues or Pull Requests by label](https://img.shields.io/github/issues/CybercentreCanada/assemblyline/core)](https://github.com/CybercentreCanada/assemblyline/issues?q=is:issue+is:open+label:core)
[![License](https://img.shields.io/github/license/CybercentreCanada/assemblyline-core)](./LICENSE.md)

# Assemblyline 4 - Core

This repository provides cores services for Assemblyline 4.

## Image variants and tags

| **Tag Type** | **Description**                                                                                  |      **Example Tag**       |
| :----------: | :----------------------------------------------------------------------------------------------- | :------------------------: |
|    latest    | The most recent build (can be unstable).                                                         |          `latest`          |
|  build_type  | The type of build used. `dev` is the latest unstable build. `stable` is the latest stable build. |     `stable` or `dev`      |
|    series    | Complete build details, including version and build type: `version.buildType`.                   | `4.5.stable`, `4.5.1.dev3` |

## Components

### Alerter

Create alerts for the different submissions in the system.

```bash
docker run --name alerter cccs/assemblyline-core python -m assemblyline_core.alerter.run_alerter
```

### Archiver

Archives submissions and their results & files into the archive.

```bash
docker run --name archiver cccs/assemblyline-core python -m assemblyline_core.archiver.run_archiver
```

### Dispatcher

Route the files in the system while a submission is tacking place. Make sure all files during a submission are completed by all required services.

```bash
docker run --name dispatcher cccs/assemblyline-core python -m assemblyline_core.dispatching
```

### Expiry

Delete submissions and their results when their time-to-live expires.

```bash
docker run --name expiry cccs/assemblyline-core python -m assemblyline_core.expiry.run_expiry
```

### Ingester

Move ingested files from the priority queues to the processing queues.

```bash
docker run --name ingester cccs/assemblyline-core python -m assemblyline_core.ingester
```

### Metrics

Generates metrics of the different components in the system.

#### Heartbeat Manager

```bash
docker run --name heartbeat cccs/assemblyline-core python -m assemblyline_core.metrics.run_heartbeat_manager
```

#### Metrics Aggregator

```bash
docker run --name metrics cccs/assemblyline-core python -m assemblyline_core.metrics.run_metrics_aggregator
```

#### Statistics Aggregator

```bash
docker run --name statistics cccs/assemblyline-core python -m assemblyline_core.metrics.run_statistics_aggregator
```

### Scaler

Spin up and down services in the system depending on the load.

```bash
docker run --name scaler cccs/assemblyline-core python -m assemblyline_core.scaler.run_scaler
```

### Updater

Make sure the different services get their latest update files.

```bash
docker run --name updater cccs/assemblyline-core python -m assemblyline_core.updater.run_updater
```

### Workflow

Run the different workflows in the system and apply their labels, priority and status.

```bash
docker run --name workflow cccs/assemblyline-core python -m assemblyline_core.workflow.run_workflow
```

## Documentation

For more information about these Assemblyline components, follow this [overview](https://cybercentrecanada.github.io/assemblyline4_docs/overview/architecture/) of the system's architecture.

---

# Assemblyline 4 - Core

Ce dépôt fournit des services de base pour Assemblyline 4.

## Variantes et étiquettes d'image

| **Type d'étiquette** | **Description**                                                                                                  |  **Exemple d'étiquette**   |
| :------------------: | :--------------------------------------------------------------------------------------------------------------- | :------------------------: |
|       dernière       | La version la plus récente (peut être instable).                                                                 |          `latest`          |
|      build_type      | Le type de compilation utilisé. `dev` est la dernière version instable. `stable` est la dernière version stable. |     `stable` ou `dev`      |
|        séries        | Le détail de compilation utilisé, incluant la version et le type de compilation : `version.buildType`.           | `4.5.stable`, `4.5.1.dev3` |

## Composants

### Alerter

Crée des alertes pour les différentes soumissions dans le système.

```bash
docker run --name alerter cccs/assemblyline-core python -m assemblyline_core.alerter.run_alerter
```

### Archiver

Archivage des soumissions, de leurs résultats et des fichiers dans l'archive.

```bash
docker run --name archiver cccs/assemblyline-core python -m assemblyline_core.archiver.run_archiver
```

### Dispatcher

Achemine les fichiers dans le système durant une soumission. S'assure que tous les fichiers de la soumission courante soient complétés par tous les services requis.

```bash
docker run --name dispatcher cccs/assemblyline-core python -m assemblyline_core.dispatching
```

### Expiration

Supprimer les soumissions et leurs résultats à l'expiration de leur durée de vie.

```bash
docker run --name expiry cccs/assemblyline-core python -m assemblyline_core.expiry.run_expiry
```

### Ingester

Déplace les fichiers ingérés des files d'attente prioritaires vers les files d'attente de traitement.

```bash
docker run --name ingester cccs/assemblyline-core python -m assemblyline_core.ingester
```

### Métriques

Génère des métriques des différents composants du système.

#### Heartbeat Manager

```bash
docker run --name heartbeat cccs/assemblyline-core python -m assemblyline_core.metrics.run_heartbeat_manager
```

#### Agrégateur de métriques

```bash
docker run --name metrics cccs/assemblyline-core python -m assemblyline_core.metrics.run_metrics_aggregator
```

##### Agrégateur de statistiques

```bash
docker run --name statistics cccs/assemblyline-core python -m assemblyline_core.metrics.run_statistics_aggregator
```

### Scaler

Augmente et diminue les services dans le système en fonction de la charge.

```bash
docker run --name scaler cccs/assemblyline-core python -m assemblyline_core.scaler.run_scaler
```

### Mise à jour

Assure que les différents services reçoivent leurs derniers fichiers de mise à jour.

```bash
docker run --name updater cccs/assemblyline-core python -m assemblyline_core.updater.run_updater
```

### Workflow

Exécute les différents flux de travail dans le système et appliquer leurs étiquettes, leur priorité et leur statut.

```bash
docker run --name workflow cccs/assemblyline-core python -m assemblyline_core.workflow.run_workflow
```

## Documentation

Pour plus d'informations sur ces composants Assemblyline, suivez ce [overview](https://cybercentrecanada.github.io/assemblyline4_docs/overview/architecture/) de l'architecture du système.
