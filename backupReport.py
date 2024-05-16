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

# Get Replica Link Status
# Return Formatted HTML code
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

    # Format DataFrame
    connectionsOutputDF = update_dataframe(connectionsOutputDF)

    make_html(connectionsOutputDF, heading)

    return(connectionsOutputDF, heading)

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
                            '<thead style="color: white; border-bottom: 5px solid #FE5000;">')
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
get_replicaStatus()     # Replica Link Status (ActiveCluster)


# Complete the HTML file
end_html_body()