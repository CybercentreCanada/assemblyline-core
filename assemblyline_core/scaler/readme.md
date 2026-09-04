

# Assemblyline Scaler

In assemblyline the scaler is responsible for:
 - Setting up service prerequisites regarding networking or dependency containers
 - Starting and stopping the service as a whole
 - Determining the number of each service that should be running at any time
 - Killing service containers that have timed out as directed by the dispatcher
 - Reporting event logs from the orchestration environment
 - Reporting metrics related to scaling for the assemblyline dashboard

## Scaling algorithm

There are many types of scaling algorithms, the one used by assemblyline falls into the 'leaky bucket' family.

Each service has a point value assocated with it, this value slowly returns to zero over time (following the metaphore of a leaking bucket). When this point value rises over some configured positive threshold it indicates that service should be scaled up. When it drops below some negative threshold it indicates the service should scale down.

The point value is increased:
 - When all the service instances are busy.
 - In proportion to the length of the service queue.

The point value is decreased:
 - When service instances are idle.

The monitoring loop that applies these changes first lists all services that should be scaled up or down. Services being scaled down have this applied immediately. In order to limit how often a service starting later is completely starved of resources the list of services that should be scaled up are considered in order starting from the service with the least number of service instances currently allocated. Services are considered for scaling up until the limits of cluster resources are reached.

