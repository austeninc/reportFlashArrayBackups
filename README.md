# Report Pure Storage FlashArray Replication Status
This UNOFFICIAL tool uses the Pure Storage REST API via the [Pure Storage FlashArray REST 1.X Python SDK](https://pure-storage-python-rest-client.readthedocs.io/en/stable/installation.html) to collect information on replication health and compile into a report.

### Current Functionality:
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

### Suggested Usage
Run this in a chron job so that replication reports are emailed to you every day.

# Installation
## Prepare the FlashArray(s)
For each FlashArray you must have the array name, the array management virtual IP, and a valid API token.
### Identify the Management IP
The management VIP can be found via `purenetwork eth list --service management` -- look for the virtual interface, likely `vir0`

Alternatively, look in the Purity//FA GUI under Settings > Network > Ethernet -- again, look for the virtual interface, likely `vir0`

### Create an API Token

An API Token must be created for every FlashArray.

**CLI**
1. Create a read-only user (in this example, the username is `readonlypython`): `pureadmin create readonlypython --role readonly`
2. Create an API token for the user: `pureadmin create readonlypython --api-token --timeout 31449600s`
3. **Save the API token for later!** You will not be able to display it again.
4. Set a reminder to update the API token in 1 year (31,449,600 seconds)
5. Verify the user is created (see example below)

```
pureuser@flasharray> pureadmin list readonlypython --api-token
Name            Type   API Token  Created                  Expires
readonlypython  local  ****       2024-05-20 09:53:15 MDT  2025-05-19 09:53:15 MDT
```

**GUI**

1. Navigate to Settings > Access
2. Select `Create User` from the Overflow Menu in the `Users` box
3. Fill out the dialog box (remember to select Role: Read Only) and click `Submit`
4. Then, select `Create API Token` from the Overflow Menu in the **row of the newly created user** (three vertical dots at the end of the row)
5. Fill out the dialog box -- set the maximum expiration time of 1 year -- and click `Submit`
6. **Save the API token for later!** You will not be able to display it again.
7. Set a reminder to update the API token in 1 year (31,449,600 seconds)

## Prepare the Host (Install Dependencies)

1. Ensure the host can reach **all** target FlashArray management IPs.
2. Install Python 3 on the host where this script will run.
3. Install the Pure Storage REST API Python library: `pip3 install purestorage`
4. Ensure other dependencies are met: `pandas`, `datetime`, `yaml`, `math`, & `smtplib`
5. Copy the contents of the latest release source code ZIP to a directory on the host. This includes: `backupReport.py`, `config.yml` and the `reports/` directory.

### Prepare the Email Function

1. Open the `backupReport.py` script for editing
2. Update the variables on lines 23, 24, 25 with the correct information for your environment -- `smtp_server`, `sender_email`, & `receiver_email` **must be configured!**

### Prepare the Arrays Config File

1. Open the `config.yaml` file for editing
2. Update the YAML with the information relevant to your environment. You **must** define all fields in the hierarchy. See example below

Note: You can define as many sites and arrays as you'd like. This is an example of the hierarchy.

```
sites:
  SITE_NAME:
    - array: ARRAY_NAME
      mgmt_ip: MANAGEMENT_IP
      api_token: API_TOKEN
    - array: ARRAY_NAME
      mgmt_ip: MANAGEMENT_IP
      api_token: API_TOKEN
  SITE_NAME:
    - array: ARRAY_NAME
      mgmt_ip: MANAGEMENT_IP
      api_token: API_TOKEN
  SITE_NAME:
    - array: ARRAY_NAME
      mgmt_ip: MANAGEMENT_IP
      api_token: API_TOKEN
```

# v1.1 Notes

### Pending Development:

* Upgrade to use the py-pure-client (https://code.purestorage.com/py-pure-client/index.html) to enable REST API v2.0+
    * The deprecated purestorage REST client only works up to REST API v1.19
* Add to Report: Sum of data replicated in 24h for each ActiveDR (async) pod
