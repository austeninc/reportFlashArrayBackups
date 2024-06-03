# Report Pure Storage FlashArray Backup Status

Use the Pure Storage REST API via the Python client to collect information on backup health and compile into a report.

## THIS PROJECT IS INCOMPLETE
### Current Function:
* Communicates with any number of arrays via REST across any number of sites
    * Requires completed YAML config file with Site & Arrays defined
* Collects status information from each array, including:
    * Capacity data
    * Array Connections status & health
    * ActiveDR (asynchronous replication) Replica Link data:
        * Status (replicating, paused, unhealthy)
        * Recovery Point
        * Lag time
    * ActiveCluster (synchronous replication)
        * Member Array(s) Status (online, offline, unknown)
        * Mediator Status (online, unreachable, unknown)
* Outputs a summarized status report for each site
* Outputs a detailed status report if any component (connection, async or sync replication) is unhealthy
* Produces an HTML report for review (see examples below)

### Pending Development:
* Add functionality to email the report to selected recipients