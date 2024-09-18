# Report Pure Storage FlashArray Backup Status

Use the Pure Storage REST API via the purestorage REST client (https://pure-storage-python-rest-client.readthedocs.io/en/stable/installation.html) to collect information on replication health and compile into a report.

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
* Produces an HTML report for review (see examples in the `reports` folder)
* Sends report via email (requires working SMTP server)

### Pending Development:
* Upgrade to use the py-pure-client (https://code.purestorage.com/py-pure-client/index.html) to enable REST API v2.0+
    * The deprecated purestorage REST client only works up to REST API v1.19
* Add to Report: Sum of data replicated in 24h for each ActiveDR (async) pod