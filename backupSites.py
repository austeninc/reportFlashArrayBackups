import purestorage
from purestorage import FlashArray

import pandas as pd

import json

import requests.packages.urllib3 # type: ignore
requests.packages.urllib3.disable_warnings() # Ignore SSL errors due to self-signed certs on Pure appliances

#-------------------------------------------#
#              Read Config JSON             #
#-------------------------------------------#
def read_json(filePath):
    with open(filePath, 'r') as file:
        jsonData = file.read()
    return jsonData

def prep_config(filePath):
    jsonData = read_json(filePath)
    config = json.loads(jsonData)
    return config
#-------------------------------------------#
#            End Config JSON Prep           #
#-------------------------------------------#

#-------------------------------------------#
#            Prepare DataFrames             #
#-------------------------------------------#
# Create Sites DF
def get_sites(config):
    # Extract Site Names
    siteNames = list(config["sites"].keys())

    # Create DataFrame
    sitesDF = pd.DataFrame(siteNames)
    sitesDF.columns = ['Site']

    # Prepare Empty Columns
    sitesDF['Arrays Count'] = pd.NA
    sitesDF['ActiveDR (async) Status'] = pd.NA
    sitesDF['ActiveCluster (sync) Status'] = pd.NA

    return sitesDF

def get_arrays(config):
    arrayDataFrames = {}
    # Prepare dataframes for arrays per site
    for site, arrays in config["sites"].items():
        arrayDataFrames[site] = pd.DataFrame(arrays)
    return arrayDataFrames
#-------------------------------------------#
#            End DataFrame Prep             #
#-------------------------------------------#

#-------------------------------------------#
#      Establish REST Session for Array     #
#-------------------------------------------#
def establish_session(mgmtIP, apiToken):
    array = purestorage.FlashArray(mgmtIP, api_token=apiToken)
    array_info = array.get()

    print("\nFlashArray {} (version {}) REST session established!\n".format(array_info['array_name'], array_info['version']))
    print(array_info, "\n")
    
    return array, array_info
#-------------------------------------------#
#       Done Establishing REST Session      #
#-------------------------------------------#

#-------------------------------------------#
#             REST API Functions            #
#-------------------------------------------#
#-------- Array Connections -------#
# List Array Connections
def list_arrayConnections(array):
    heading = "Array Connections"

    connections = array.list_array_connections()

    connectionsDF = pd.DataFrame(connections)

    #print(f"{heading}\n{connectionsDF}")

    if connectionsDF.empty == True:
        connectionsStatus = "N/A"
        return(heading, connectionsDF, connectionsStatus)
    if (connectionsDF['status'] == "connected").all():
        connectionsStatus = "Okay"
        return(heading, connectionsDF, connectionsStatus)
    else:
        connectionsStatus = "Warning"
        return(heading, connectionsDF, connectionsStatus)

# Return Formatted HTML code
def format_arrayConnections(arrayName, heading, connectionsDF):
    # Specify the new column order
    new_order = ['array_name', 'version', 'type', 'status', 'throttled', 'management_address', 'replication_address', 'id']

    # Reorder the DataFrame columns
    connectionsOutputDF = connectionsDF[new_order]

    # Sort by Direction
    connectionsOutputDF = connectionsOutputDF.sort_values(by=['status', 'array_name'], ascending=False)

    # Rename 'array_name' to clearly be Remote
    connectionsOutputDF = connectionsOutputDF.rename(columns={'array_name': 'remote_names'})

    # Format DataFrame
    #connectionsOutputDF = update_dataframe(connectionsOutputDF)

    #make_html(heading, connectionsOutputDF)

    print(f"{arrayName}: {heading}\n{connectionsOutputDF}")

    return(heading, connectionsOutputDF)
#------ End Array Connections -----#
#-------------------------------------------#
#          End REST API Functions           #
#-------------------------------------------#

#-------------------------------------------#
#            Prepare HTML Output            #
#-------------------------------------------#
# Convert DataFrame to HTML
def make_html(heading, headingStyle, dataframe):
    dataFrameHTML = dataframe.to_html(index=False)

    if headingStyle == 1:
        headingHTML = "<h1>" + heading + "</h1>\n\n"
    if headingStyle == 2:
        headingHTML = "<h2>" + heading + "</h2>\n\n"
    if headingStyle == 3:
        headingHTML = "<h3>" + heading + "</h3>\n\n"
    
    heading, htmlOutput = format_table(headingHTML, headingStyle, dataFrameHTML)

    write_html(heading, htmlOutput)

    return(heading, htmlOutput)

# Format any HTML table
def format_table(heading, headingStyle, htmlInput):

    if headingStyle == 1:
        headingHTML = heading.replace('<h1>',
                                        '\n<h1 style="color: white; width: 100%; font-family: Arial, sans-serif; font-size: 1.25em;">')
    if headingStyle == 2:
        headingHTML = heading.replace('<h2>',
                                        '\n<h2 style="color: white; width: 100%; font-family: Arial, sans-serif; font-size: 1.25em;">')
    if headingStyle == 3:
        headingHTML = heading.replace('<h3>',
                                        '\n<h3 style="color: white; width: 100%; font-family: Arial, sans-serif; font-size: 1em;">')

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
    return(headingHTML, html)
#-------------------------------------------#
#        Done Preparing HTML Output         #
#-------------------------------------------#

#-------------------------------------------#
#            Write HTML to File             #
#-------------------------------------------#
# Write output to HTML file
## Create the file
def start_html_body():
    with open('test_sites_output.html', 'w') as f:
        f.write("<body style=\"background-color: #1C1C1C; padding-top: 2vh; padding-left: 7vw; padding-right: 8vw;\">\n")
        f.write("<img src='assets/pstg_logo_darkMode.svg' width=250 /><br /><br />")

## Add site summary to the file
def site_summary_to_html():
    with open('test_sites_output.html', 'a') as f:
        f.write("<h1>Summary</h1>")

## Add tables to HTML
def write_html(title, html):
    with open('test_sites_output.html', 'a') as f:
        f.writelines(title)
        f.writelines(html)
        f.writelines('</br></br>')

## Close the file with ending body tag
def end_html_body():
    with open('test_sites_output.html', 'a') as f:
        f.writelines("\n</body>")

#############################################
##-----------------------------------------##
##               Run Program               ##
##-----------------------------------------##
#############################################

# Read the config file and transform to JSON data
configJSON = "config.json"
config = prep_config(configJSON)

# Prepare DataFrames for each Site and the Arrays in each site
sitesDF = get_sites(config)
arraysDF = get_arrays(config)

print(sitesDF)

start_html_body()
# Summarize Sites in HTML
summaryHeading = "Summary"
summaryHeadingStyle = 1
make_html(summaryHeading, summaryHeadingStyle, sitesDF)

for site, arrays in arraysDF.items(): # Iterate through sitesDF to collect data
    print(f"\nDataframe for {site}:\n", arrays)
    siteArraysDF = arrays

    # Add new columns for array_name and purity_version if not already present
    if 'array_name' not in siteArraysDF.columns:
        siteArraysDF['array_name'] = pd.NA
    if 'purity_version' not in siteArraysDF.columns:
        siteArraysDF['purity_version'] = pd.NA
    if 'used_capacity' not in siteArraysDF.columns:
        siteArraysDF['used_capacity'] = pd.NA
    if 'drr' not in siteArraysDF.columns:
        siteArraysDF['drr'] = pd.NA
    if 'array_connections_status' not in siteArraysDF.columns:
        siteArraysDF['array_connections_status'] = pd.NA
    if 'activeDR_status' not in siteArraysDF.columns:
        siteArraysDF['activeDR_status'] = pd.NA
    if 'activeCluster_status' not in siteArraysDF.columns:
        siteArraysDF['activeCluster_status'] = pd.NA

    for index, row in siteArraysDF.iterrows(): # Iterate through arraysDF to collect data for each array in each site

        ### Establish REST API session ###
        mgmtIP = row['mgmt_ip']
        apiToken = row['api_token']

        array, array_info = establish_session(mgmtIP, apiToken)

        # Variables for this Iteration
        arrayName = array_info['array_name']
        purityVersion = array_info['version']

        # Update siteArraysDF with array_name & purity_version
        siteArraysDF.at[index, 'array_name'] = arrayName
        siteArraysDF.at[index, 'purity_version'] = purityVersion
        #### End establish REST API session ###

        ### Get Capacity Information ###
        ### End Capacity Information ###

        ### Get Array Connection Information ###
        connectionsHeading, connectionsDF, connectionsStatus = list_arrayConnections(array)

        if connectionsDF.empty == True:
            siteArraysDF.at[index, 'array_connections_status'] = connectionsStatus
        if connectionsStatus == "Okay":
            siteArraysDF.at[index, 'array_connections_status'] = connectionsStatus
        if connectionsStatus == "Warning":
            siteArraysDF.at[index, 'array_connections_status'] = connectionsStatus
            format_arrayConnections(arrayName, connectionsHeading, connectionsDF)
        ### End Array Connection Information ###

        ### Get ActiveDR (async) Information ###
        ### End ActiveDR (async) Information ###

        ### Get ActiveCluster (sync) Information ###
        ### End ActiveCluster (sync) Information ###

        print(siteArraysDF)
    
    #### Format DataFrames ####
    # Remove array, mgmt_ip and api_token columns from siteArraysDF
    siteArraysDF = siteArraysDF.drop(columns=['array', 'mgmt_ip', 'api_token'])
    print(siteArraysDF)
    arraysHeading = (f"{site} Arrays")
    arraysHeadingStyle = 2
    make_html(arraysHeading, arraysHeadingStyle, siteArraysDF)


end_html_body()