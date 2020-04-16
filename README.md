# Assemblyline 4 - Core services

This repository provides cores services for Assemblyline 4.

#### Core Services 

##### Alerter 

Create alerts for the different submissions in the system.

##### Dispatcher

Route the files in the system while a submission is tacking place. Make sure all files during a submission are completed by all required services.

##### Expiry

Delete submissions and their results when their TTL expires.

##### Ingester

Move ingested files from the priority queues to the processing queues.

##### Metrics

Generates metrics of the different components in the system.

##### Scaler

Spin up and down services in the system depending on the load.

##### Updater

Make sure the different services get their latest update files.

##### Workflow

Run the different workflows in the system and apply their labels, priority and status.
