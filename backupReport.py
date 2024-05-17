import purestorage
from purestorage import FlashArray

import pandas as pd

import datetime

import requests.packages.urllib3 # type: ignore
requests.packages.urllib3.disable_warnings()


# Validate Connection & Output Info
def establish_session():
    print("\nFlashArray {} (version {}) REST session established!\n".format(array_info['array_name'], array_info['version']))
    print(array_info, "\n")

# List Array Connections
## Return Formatted HTML code
def list_arrayConnections():
    heading = "Array Connections"

    connections = array.list_array_connections()

    connectionsDF = pd.DataFrame(connections)

    # Specify the new column order
    new_order = ['array_name', 'version', 'type', 'status', 'throttled', 'management_address', 'replication_address', 'id']

    # Reorder the DataFrame columns
    connectionsOutputDF = connectionsDF[new_order]

    # Sort by Direction
    connectionsOutputDF = connectionsOutputDF.sort_values(by=['status', 'array_name'], ascending=False)

    # Rename 'array_name' to clearly be Remote
    connectionsOutputDF = connectionsOutputDF.rename(columns={'array_name': 'remote_names'})

    # Format DataFrame
    connectionsOutputDF = update_dataframe(connectionsOutputDF)

    make_html(connectionsOutputDF, heading)

    return(connectionsOutputDF, heading)

# Get Replica Link Status
## Return Formatted HTML code
def get_replicaStatus():

    heading = "Replica Link Status"

    replicas = array.list_pod_replica_links()

    replicasDF = pd.DataFrame(replicas)

    # Specify the new column order
    new_order = ['local_pod_name', 'direction', 'remote_names', 'remote_pod_name', 'status', 'recovery_point', 'lag']

    # Reorder the DataFrame columns
    replicasOutputDF = replicasDF[new_order]

     # Sort by Direction
    replicasOutputDF = replicasOutputDF.sort_values(by=['local_pod_name', 'direction'], ascending=True)

    # Format DataFrame
    replicasOutputDF = update_dataframe(replicasOutputDF)

    make_html(replicasOutputDF, heading)

    return(replicasOutputDF, heading)

# List All Pods
## Return Formatted HTML code
def list_pods():
    heading = "Active Cluster Pods"

    pods = array.list_pods()

    podsDF = pd.DataFrame(pods)

    ####### Clean Up Output #########
    # Filter out non-replicating pods
    podsFilteredDF = podsDF[podsDF['arrays'].apply(len) > 1]

    # Drop unwanted columns from Pods table
    podsFilteredDF = podsFilteredDF.drop(columns=['link_source_count', 'link_target_count', 'requested_promotion_state', 'source'])

    # Re-Order the Columns
    new_order = ['name', 'promotion_status', 'arrays']
    podsFilteredDF = podsFilteredDF[new_order]

    # Explode the 'arrays' column
    podsArrays = podsFilteredDF.explode('arrays').reset_index(drop=True)

    # Normalize the exploded 'arrays' column
    podsNormalizedArrays = pd.json_normalize(podsArrays['arrays'])

    # Drop unwanted columns from normalized arrays
    podsNormalizedArrays = podsNormalizedArrays.drop(columns=['pre_elected', 'frozen_at', 'progress', 'array_id'])

    # Clean up column names to clearly state "array_name"
    podsNormalizedArrays = podsNormalizedArrays.rename(columns={'name': 'array_name'})

    # Add a 'pod_' prefix to column names for clarity
    podsArrays = podsArrays.add_prefix('pod_')

    # Repeat the rows in the original DataFrame to match the length of the exploded arrays
    podsDuplicatedDF = podsArrays.drop(columns=['pod_arrays']).reset_index(drop=True)

    # Concatenate the repeated rows with the normalized arrays
    podsFilteredDF = pd.concat([podsDuplicatedDF, podsNormalizedArrays], axis=1)

    # Replace duplicate data with empty strings
    cols_to_check = ['pod_name', 'pod_promotion_status']
    podsFilteredDF.loc[:, cols_to_check] = podsFilteredDF.loc[:, cols_to_check].mask(podsFilteredDF.loc[:, cols_to_check].duplicated(), '')
    ###### Clean Up Complete ########


    # Prep for HTML
    podsOutputDF = update_dataframe(podsFilteredDF)

    make_html(podsOutputDF, heading)

    return(podsOutputDF, heading)


# Rename Columns & Cells
## Return updated DataFrame
def update_dataframe(input):

    df = input
    df = df.copy()
    
    # Convert Timestamps to Human Readable
    if 'recovery_point' in df:
        df['recovery_point'] = pd.to_datetime(df['recovery_point'], unit='ms', utc=True)    # Assume UTC source
        df['recovery_point'] = df['recovery_point'].dt.tz_convert('America/Los_Angeles')    # Assume convert to Pacific time
        df['recovery_point'] = df['recovery_point'].dt.strftime('%Y-%m-%d %H:%M:%S %Z')     # Make human readable

    # Convert Lag Time ms to seconds

    # Convert Alert opened Timestamps to standard format
    if 'opened' in df:
        df['opened'] = pd.to_datetime(df['opened'], format='%Y-%m-%dT%H:%M:%SZ') #.dt.tz_localize('UTC')
        #df['opened'] = df['opened'].dt.tz_convert('America/Los_Angeles')
        df['opened'] = df['opened'].dt.strftime('%Y-%m-%d %H:%M:%S %Z')


    # Replace Column Titles with Human-Readable (Dictionary)
    df.rename(columns={'version': 'Version',
                     'throttled': 'Throttled',
                     'status': 'Status',
                     'management_address': 'Management IP',
                     'id': 'Array ID',
                     'array_name': 'Array',
                     'replication_address': 'Replication IP',
                     'type': 'Replication Type',
                     'local_pod_name': 'Local Pod',
                     'remote_names': 'Remote Array',
                     'remote_pod_name': 'Remote Pod',
                     'recovery_point': 'Recovery Point',
                     'direction': 'Direction',
                     'lag': 'Lag',
                     'pod_name': 'Pod',
                     'pod_source': 'Source',
                     'pod_promotion_status': 'Promotion Status',
                     'mediator_status': 'Mediator Status',
                     'alert_id': 'Alert ID',
                     'current_severity': 'Severity',
                     'opened': 'Opened',
                     'alert_code': 'Alert Code',
                     'component_type': 'Component',
                     'event': 'Event',
                     'alert_details': 'Details'
                    }, inplace=True)
    
    # Replace 'Engineer' with 'Software Engineer' in the 'Occupation' column
    df = df.replace({'replicating': 'Replicating',
                    'outbound': '\N{RIGHTWARDS ARROW}',
                    'inbound': '\N{LEFTWARDS ARROW}'
                    })

    return(df)

    

# Convert DataFrame to HTML
def make_html(dataframe, heading):
    dataFrameHTML = dataframe.to_html(index=False)

    headingHTML = "<h2>" + heading + "</h2>\n\n"
    
    htmlOutput, heading = format_table(dataFrameHTML, headingHTML)

    write_html(htmlOutput, heading)

    return(htmlOutput, heading)

# Format any HTML table
def format_table(htmlInput, heading):

    headingHTML = heading.replace('<h2>',
                                    '\n<h2 style="color: white; width: 100%; font-family: Arial, sans-serif; font-size: 1.25em;">')

    html = htmlInput.replace('<table border="1" class="dataframe">',
                                '<table style="border-collapse: collapse; width: 100%; font-family: Arial, sans-serif;">')
    html = html.replace('<thead>',
                            '<thead style="color: white; border-bottom: 3px solid #FE5000;">')
    html = html.replace('<th>',
                            '<th style="color:white; border-bottom: 1px solid #FE5000; text-align: left; padding: 8px;">')
    html = html.replace('<td>',
                            '<td style="color:white; border-bottom: 1px solid #DADADA; text-align: left; padding: 8px;">')
    #html = html.replace('<tr>',
    #                        '<tr style="background-color: #6C6C6C;">')
    html = html.replace('<tr>',
                            '<tr style="color:white; background-color: #1C1C1C;">')
    html = html.replace('</table>',
                            '</table>\n')

    #print(htmlInput)
    return(html, headingHTML)

# Write output to HTML file
## Create the file
def start_html_body():
    with open('test_output.html', 'w') as f:
        f.write("<body style=\"background-color: #1C1C1C; padding-top: 2vh; padding-left: 7vw; padding-right: 8vw;\">\n")
        f.write("<img src='assets/pstg_logo_darkMode.svg' width=250 /><br /><br />")

## Add tables to HTML
def write_html(html, title):
    with open('test_output.html', 'a') as f:
        f.writelines(title)
        f.writelines(html)
        f.writelines('</br></br>')

## Close the file with ending body tag
def end_html_body():
    with open('test_output.html', 'a') as f:
        f.writelines("\n</body>")

################
# Run Program  #
################

# Initialize Array
array = purestorage.FlashArray("10.235.116.230", api_token="26d6ca06-3496-6f9a-936e-5c88cd6ed359")
array_info = array.get()

establish_session()

# Initialize HTML file
start_html_body()

# Compile Data
list_arrayConnections() # Array Connections
get_replicaStatus()     # Replica Link Status (ActiveDR)
list_pods()             # List replicating pods (ActiveCluster)


# Complete the HTML file
end_html_body()



##### If Mediator is unhealthy: https://support.purestorage.com/bundle/m_activecluster/page/FlashArray/PurityFA/Replication/ActiveCluster/Troubleshooting/topics/concept/c_impact_312.html