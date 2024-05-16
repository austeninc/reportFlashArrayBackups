import purestorage
from purestorage import FlashArray

import pandas as pd

from datetime import datetime

import requests.packages.urllib3 # type: ignore
requests.packages.urllib3.disable_warnings()

# Validate Connection & Output Info
def establish_session():
    print("\nFlashArray {} (version {}) REST session established!\n".format(array_info['array_name'], array_info['version']))
    print(array_info, "\n")

# List Array Connections
def list_arrayConnections():
    heading = "Array Connections"

    connections = array.list_array_connections()

    print("\n\n", heading, "\n")
    for r in range(len(connections)):
        print(connections[r], "\n")

    connectionsDF = pd.DataFrame(connections)

    # Specify the new column order
    new_order = ['array_name', 'version', 'type', 'status', 'throttled', 'management_address', 'replication_address', 'id']

    # Reorder the DataFrame columns
    connectionsOutputDF = connectionsDF[new_order]

    # Sort by Direction
    connectionsOutputDF = connectionsOutputDF.sort_values(by=['status', 'array_name'], ascending=False)

    # Format DataFrame
    connectionsOutputDF = update_dataframe(connectionsOutputDF)

    print(connectionsOutputDF)

    return(connectionsOutputDF, heading)

# List All Pods
def list_pods():
    heading = "Pods"

    pods = array.list_pods()

    print("\n\n", heading, "\n")
    for r in range(len(pods)):
        print(pods[r], "\n")

    podsDF = pd.DataFrame(pods)

    print(podsDF)


# Get Replica Link Status
def get_replicaStatus():
    heading = "Replica Link Status"

    replicas = array.list_pod_replica_links()

    print("\n\n", heading, "\n")
    for r in range(len(replicas)):
        print(replicas[r], "\n")

    replicasDF = pd.DataFrame(replicas)

    print(replicasDF)

    # Specify the new column order
    new_order = ['local_pod_name', 'direction', 'remote_names', 'remote_pod_name', 'status', 'recovery_point', 'lag']

    # Reorder the DataFrame columns
    replicasOutputDF = replicasDF[new_order]

     # Sort by Direction
    replicasOutputDF = replicasOutputDF.sort_values(by=['local_pod_name', 'direction'], ascending=True)

    # Format DataFrame
    replicasOutputDF = update_dataframe(replicasOutputDF)

    print(replicasOutputDF)

    return(replicasOutputDF, heading)

def update_dataframe(input):

    df = input

    # Replace Column Titles with Human-Readable (Dictionary)
    df.rename(columns={'version': 'Version',
                     'throttled': 'Throttled',
                     'status': 'Status',
                     'management_address': 'Management IP',
                     'id': 'Array ID',
                     'array_name': 'Remote Array',
                     'replication_address': 'Replication IP',
                     'type': 'Replication Type',
                     'local_pod_name': 'Local Pod',
                     'remote_names': 'Remote Array',
                     'remote_pod_name': 'Remote Pod',
                     'recovery_point': 'Recovery Point',
                     'direction': 'Direction',
                     'lag': 'Lag'
                    }, inplace=True)
    
    # Replace 'Engineer' with 'Software Engineer' in the 'Occupation' column
    df = df.replace({'replicating': 'Replicating',
                    'outbound': '\N{RIGHTWARDS ARROW}',
                    'inbound': '\N{LEFTWARDS ARROW}'
                    })
    
    # Convert Timestamps to Human Readable
    if 'Recovery Point' in df:
        df['Recovery Point'] = pd.to_datetime(df['Recovery Point'], unit='ms', utc=True)    # Assume UTC source
        df['Recovery Point'] = df['Recovery Point'].dt.tz_convert('America/Los_Angeles')    # Assume convert to Pacific time
        df['Recovery Point'] = df['Recovery Point'].dt.strftime('%Y-%m-%d %H:%M:%S %Z')     # Make human readable

    return(df)

################
# Run Program  #
################

# Initialize Array
array = purestorage.FlashArray("10.235.116.230", api_token="26d6ca06-3496-6f9a-936e-5c88cd6ed359")
array_info = array.get()

establish_session()

# Compile Data
list_arrayConnections() # Array Connections
get_replicaStatus()     # Replica Link Status (ActiveCluster)
list_pods()             # List Pods