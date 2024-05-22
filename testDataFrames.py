import purestorage
from purestorage import FlashArray

import pandas as pd

import yaml
import json

import requests.packages.urllib3 # type: ignore
requests.packages.urllib3.disable_warnings() # Ignore SSL errors due to self-signed certs on Pure appliances

#-------------------------------------------#
#              Global Variables             #
#-------------------------------------------#
#-------------------------------------------#
#            End Global Variables           #
#-------------------------------------------#

#############################################
##-----------------------------------------##
##               Run Program               ##
##-----------------------------------------##
#############################################

#-------------------------------------------#
#              Read Config YAML             #
#-------------------------------------------#
def read_yaml(filePath):
    with open(filePath, 'r') as file:
        yamlData = yaml.safe_load(file)
    return yamlData

# Read the config file and transform to JSON data
configYAML = "config.yaml"
yamlData = read_yaml(configYAML)
#-------------------------------------------#
#            End Config YAML Prep           #
#-------------------------------------------#

#-------------------------------------------#
#            Prepare DataFrames             #
#-------------------------------------------#
# Create a "Summary" dataframe containing only the site names
siteData = {'site': list(yamlData['sites'].keys())}
siteSummary = pd.DataFrame(siteData)

# Create nested DataFrames for array output
def create_nested_dataframes(arrays):
    # Assuming no keys for now
    capacity_data = [{'array': array['array'], 
                      'percent_full': array.get('capacity', 'N/A'), 
                      'drr': array.get('drr', 'N/A'), 
                      'space_used': array.get('space_used', 'N/A'), 
                      'total_usable': array.get('total_usable', 'N/A')} for array in arrays]
    connection_data = [{'array': array['array'], 
                        'connection_status': array.get('connection_status', 'N/A'),
                        'connection_data': array.get('connection_data', 'N/A')} for array in arrays]
    activeDR_data = [{'array': array['array'], 
                      'activeDR_status': array.get('activeDR_status', 'N/A'),
                      'activeDR_data': array.get('activeDR_data', 'N/A')} for array in arrays]
    activeCluster_data = [{'array': array['array'], 
                           'activeCluster_status': array.get('activeCluster_status', 'N/A'),
                           'activeCluster_data': array.get('activeCluster_data', 'N/A')} for array in arrays]

    capacity_df = pd.DataFrame(capacity_data)
    connection_df = pd.DataFrame(connection_data)
    activeDR_df = pd.DataFrame(activeDR_data)
    activeCluster_df = pd.DataFrame(activeCluster_data)

    return capacity_df, connection_df, activeDR_df, activeCluster_df

# Function to create additional nested DataFrame for each array in connection_df
def create_nested_connection_df(array_name):
    # Example data for the additional nested DataFrame
    additional_data = [{'local_array_name': f'local_array_name for {array_name}', 
                        'local_version': f'local_version for {array_name}',
                        'status': f'status for {array_name}',
                        'remote_names': f'remote_names for {array_name}',
                        'remote_version': f'v for {array_name}',
                        'type': f'type for {array_name}',
                        'throttled': f'throttled for {array_name}',
                        'management_address': f'management_address for {array_name}',
                        'replication_address': f'replication_address for {array_name}'}]
    additional_connection_df = pd.DataFrame(additional_data)
    return additional_connection_df

# Function to create additional nested DataFrame for each array in activeDR_df
def create_nested_activeDR_df(array_name):
    # Example data for the additional nested DataFrame
    additional_data = [{'subfield1': f'subvalue1 for {array_name}', 'subfield2': f'subvalue2 for {array_name}'}]
    additional_activeDR_df = pd.DataFrame(additional_data)
    return additional_activeDR_df

# Function to create additional nested DataFrame for each array in activeCluster_df
def create_nested_activeCluster_df(array_name):
    # Example data for the additional nested DataFrame
    additional_data = [{'subfield1': f'subvalue1 for {array_name}', 'subfield2': f'subvalue2 for {array_name}'}]
    additional_activeCluster_df = pd.DataFrame(additional_data)
    return additional_activeCluster_df

# Dynamically create dataframes for each site
site_arrays_dfs = {}
nested_site_dfs = {}

for site, arrays in yamlData['sites'].items():
    arrays_df = pd.DataFrame(arrays)
    site_arrays_dfs[site] = arrays_df
    capacity_df, connection_df, activeDR_df, activeCluster_df = create_nested_dataframes(arrays)

    # Add an additional nested DataFrame within connection_df for each array
    connection_nested_dfs = {}
    for index, row in connection_df.iterrows():
        additional_connection_df = create_nested_connection_df(row['array'])
        connection_nested_dfs[row['array']] = additional_connection_df

    # Add an additional nested DataFrame within activeDR_df for each array
    activeDR_nested_dfs = {}
    for index, row in activeDR_df.iterrows():
        additional_activeDR_df = create_nested_activeDR_df(row['array'])
        activeDR_nested_dfs[row['array']] = additional_activeDR_df

    # Add an additional nested DataFrame within activeCluster_df for each array
    activeCluster_nested_dfs = {}
    for index, row in activeCluster_df.iterrows():
        additional_activeCluster_df = create_nested_activeCluster_df(row['array'])
        activeCluster_nested_dfs[row['array']] = additional_activeCluster_df

    nested_site_dfs[site] = {
        'arrays': arrays_df,
        'capacity': capacity_df,
        'connection_status': connection_df,
        'activeDR_status': activeDR_df,
        'activeCluster_status': activeCluster_df
    }
#-------------------------------------------#
#            End DataFrame Prep             #
#-------------------------------------------#

#-------------------------------------------#
#      Establish REST Session for Array     #
#-------------------------------------------#
def establish_session(mgmtIP, apiToken):
    array = purestorage.FlashArray(mgmtIP, api_token=apiToken)
    array_info = array.get()

    #print("\nFlashArray {} (version {}) REST session established!\n".format(array_info['array_name'], array_info['version']))
    #print(array_info, "\n")
    
    return array, array_info
#-------------------------------------------#
#       Done Establishing REST Session      #
#-------------------------------------------#

#-------------------------------------------#
#        Get Array Space Consumption        #
#-------------------------------------------#
def capacity_calculations(array_capacity_df):
    array_consumed = array_capacity_df.at[0, 'total']
    array_usable = array_capacity_df.at[0, 'capacity']
    array_percent_full = array_consumed / array_usable * 100
    array_drr = array_capacity_df.at[0, 'data_reduction']

    #print(f"Array Space: {array_capacity}\nPercent Full: {array_percent_full}%\n\nUsed: {array_consumed} \nPhysical: {array_usable} \nDRR: {array_drr}\n")

    # Calculations
    ## Percent full
    new_capacity_value = array_percent_full

    return array_consumed, array_usable, array_percent_full, array_drr

# Iterate through the arrays dataframe for every site and update the capacity dataframe
for site, dfs in nested_site_dfs.items():
    print(f"Iterating through arrays for site: {site}")
    arrays_df = dfs['arrays']
    capacity_df = dfs['capacity']
    for index, row in arrays_df.iterrows():
        #print(f"Row: {row}\n")
        mgmtIP = row['mgmt_ip']
        apiToken = row['api_token']
        #print(f"mgmt_ip: {mgmtIP}, api_token: {apiToken}\n")

        array, array_info = establish_session(mgmtIP, apiToken)

        #print(f"Array Info: {array_info}\n")

        # Temporary array capacity DF for calculations
        array_capacity = array.get(space=True)
        array_capacity_df = pd.DataFrame(array_capacity)

        # Calculate Capacity Values
        array_consumed, array_usable, array_percent_full, array_drr = capacity_calculations(array_capacity_df)

        # Update capacity dataframe for site with array stats
        capacity_df.at[index, 'percent_full'] = array_percent_full ## Percent Full (percentage)
        capacity_df.at[index, 'drr'] = array_drr ## DRR
        capacity_df.at[index, 'space_used'] = array_consumed ## Consumed (bytes)
        capacity_df.at[index, 'total_usable'] = array_usable ## Total Usable Capacity (bytes)

        # Printing to show the update
        #print(f"Updated capacity for array {row['array']} to {array_percent_full}")
#-------------------------------------------#
#        End Array Space Consumption        #
#-------------------------------------------#

#-------------------------------------------#
#           Get Array Connections           #
#-------------------------------------------#
# Iterate through the arrays dataframe for every site and get array connections
# Iterate through the connection_nested dataframes and update them
for site, dfs in nested_site_dfs.items():
    print(f"Updating nested Connections DataFrames for site: {site}")
    arrays_df = dfs['arrays']
    connections_df = dfs['connection_status']
    # Iterate through arrays to get Array Connection Information
    for index, row in arrays_df.iterrows():

        print(f"Updating nested Connections DataFrames for array: {row['array']}")

        mgmtIP = row['mgmt_ip']
        apiToken = row['api_token']

        array, array_info = establish_session(mgmtIP, apiToken)

        array_connections_data = array.list_array_connections()
        array_connections_df = pd.DataFrame(array_connections_data)

        if array_connections_df.empty == True:
            connections_df.at[index, 'connection_status'] = array_connections_data
        elif (array_connections_df['status'] == 'conneted').all():
            connections_df.at[index, 'connection_status'] = "Okay"
            connections_df.at[index, 'connection_data'] = array_connections_data
            print(array_connections_df)
        else:
            connections_df.at[index, 'connection_status'] = "Warning"
            connections_df.at[index, 'connection_data'] = array_connections_data
            print(array_connections_df)
#-------------------------------------------#
#           End Array Connections           #
#-------------------------------------------#

#-------------------------------------------#
#             Get ActiveDR Pods             #
#-------------------------------------------#
# Iterate through the arrays dataframe for every site and get ActiveDR pods
# Iterate through the activeDR_nested dataframes and update them
for site, dfs in nested_site_dfs.items():
    print(f"Updating nested ActiveDR DataFrames for site: {site}")
    arrays_df = dfs['arrays']
    activeDR_df = dfs['activeDR_status']

    # Iterate through arrays to get Array ActiveDR Information
    for index, row in arrays_df.iterrows():

        print(f"Updating nested ActiveDR DataFrames for array: {row['array']}")

        mgmtIP = row['mgmt_ip']
        apiToken = row['api_token']

        array, array_info = establish_session(mgmtIP, apiToken)

        array_activeDR_data = array.list_pod_replica_links()
        array_activeDR_df = pd.DataFrame(array_activeDR_data)

        if array_activeDR_df.empty == True:
            activeDR_df.at[index, 'activeDR_data'] = array_activeDR_data
        elif (array_activeDR_df['status'] == 'replicating').all():
            activeDR_df.at[index, 'activeDR_status'] = "Okay"
            activeDR_df.at[index, 'activeDR_data'] = array_activeDR_data
            print(array_activeDR_df)
        else:
            activeDR_df.at[index, 'activeDR_status'] = "Warning"
            activeDR_df.at[index, 'activeDR_data'] = array_activeDR_data
            print(array_activeDR_df)
#-------------------------------------------#
#             End ActiveDR Pods             #
#-------------------------------------------#

#-------------------------------------------#
#           Get ActiveCluster Pods          #
#-------------------------------------------#
# Iterate through the arrays dataframe for every site and get ActiveDR pods
# Iterate through the activeDR_nested dataframes and update them
for site, dfs in nested_site_dfs.items():
    print(f"Updating nested ActiveCluster DataFrames for site: {site}")
    arrays_df = dfs['arrays']
    activeCluster_df = dfs['activeCluster_status']

    # Iterate through arrays to get Array ActiveDR Information
    for index, row in arrays_df.iterrows():

        print(f"Updating nested ActiveCluster DataFrames for array: {row['array']}")

        mgmtIP = row['mgmt_ip']
        apiToken = row['api_token']

        array, array_info = establish_session(mgmtIP, apiToken)

        array_podsAC_data = array.list_pods()


        # Filter the ResponseList to remove entries where 'arrays' has 1 or fewer entries
        array_activeCluster_data = [entry for entry in array_podsAC_data if len(entry['arrays']) > 1]

        # Create DataFrame with filtered data
        array_activeCluster_df = pd.DataFrame(array_activeCluster_data)


        #print(array_activeCluster_data)
        #print(array_activeCluster_df)

        for idx, line in array_activeCluster_df.iterrows():
            activeCluster_members_data = line['arrays']
            activeCluster_members_df = pd.DataFrame(activeCluster_members_data)

            if activeCluster_members_df.empty == True:
                activeCluster_df.at[index, 'activeCluster_data'] = array_podsAC_data
            elif (activeCluster_members_df['status'] == 'online').all():
                activeCluster_df.at[index, 'activeCluster_status'] = "Okay"
                activeCluster_df.at[index, 'activeCluster_data'] = array_podsAC_data
                print(array_activeCluster_df)
            else:
                activeCluster_df.at[index, 'activeCluster_status'] = "Warning"
                activeCluster_df.at[index, 'activeCluster_data'] = array_podsAC_data
                print(array_activeCluster_df)
#-------------------------------------------#
#           End ActiveCluster Pods          #
#-------------------------------------------#
#-------------------------------------------#
#           Process the DataFrames          #
#-------------------------------------------#

# Optionally, if you want to store them in variables for easy access
summary_dataframe = siteSummary
site_dataframes = site_arrays_dfs
nested_site_dataframes = nested_site_dfs
#"""

# Add Connected Arrays Status column to siteSummary
siteSummary['site_connections_status'] = "N/A"

# Update 'site_connections_status' in siteSummary based on array connections status in nested dataframes
for site, dfs in nested_site_dfs.items():
    print(f"{site} - Connection Status DataFrame:")
    print(dfs['connection_status'])
    print()

    status_for_siteSummary = "Okay"

    tempDFS = dfs['connection_status']

    if (tempDFS['connection_status'] == "Warning").any():
        if status_for_siteSummary == "Okay":
            status_for_siteSummary = "Warning"
        print(f"Status for {site} is {status_for_siteSummary}")
    else:
        print(f"Status for {site} is {status_for_siteSummary}")

    # Update the site_connections_status in siteSummary for the current site
    siteSummary.loc[siteSummary['site'] == site, 'site_connections_status'] = status_for_siteSummary

print("Updated Summary Dataframe:")
print(siteSummary)


# Add ActiveDR Status column to siteSummary
siteSummary['site_activeDR_status'] = "N/A"

# Update 'site_activeDR_status' in siteSummary based on activeDR status in nested dataframes
for site, dfs in nested_site_dfs.items():
    print(f"{site} - ActiveDR Status DataFrame:")
    print(dfs['activeDR_status'])
    print()

    status_for_siteSummary = "Okay"

    tempDFS = dfs['activeDR_status']

    if (tempDFS['activeDR_status'] == "Warning").any():
        if status_for_siteSummary == "Okay":
            status_for_siteSummary = "Warning"
        print(f"Status for {site} is {status_for_siteSummary}")
    else:
        print(f"Status for {site} is {status_for_siteSummary}")

    # Update the site_connections_status in siteSummary for the current site
    siteSummary.loc[siteSummary['site'] == site, 'site_activeDR_status'] = status_for_siteSummary

print("Updated Summary Dataframe:")
print(siteSummary)

# Add ActiveCluster Status column to siteSummary
siteSummary['site_activeCluster_status'] = "N/A"

# Update 'site_activeDR_status' in siteSummary based on activeDR status in nested dataframes
for site, dfs in nested_site_dfs.items():
    print(f"{site} - ActiveCluster Status DataFrame:")
    print(dfs['activeCluster_status'])
    print()

    status_for_siteSummary = "Okay"

    tempDFS = dfs['activeCluster_status']

    if (tempDFS['activeCluster_status'] == "Warning").any():
        if status_for_siteSummary == "Okay":
            status_for_siteSummary = "Warning"
        print(f"Status for {site} is {status_for_siteSummary}")
    else:
        print(f"Status for {site} is {status_for_siteSummary}")

    # Update the site_connections_status in siteSummary for the current site
    siteSummary.loc[siteSummary['site'] == site, 'site_activeCluster_status'] = status_for_siteSummary

print("Updated Summary Dataframe:")
print(siteSummary)

#-------------------------------------------#
#           End DataFrame Processing        #
#-------------------------------------------#

# Print the dataframes

print("Summary DataFrame:")
print(siteSummary)
print()

for site, dfs in nested_site_dfs.items():
    print(f"{site} - Arrays DataFrame:")
    print(dfs['arrays'])
    print()
    print(f"{site} - Capacity DataFrame:")
    print(dfs['capacity'])
    print()
    if (siteSummary.loc[siteSummary['site'] == site, 'site_connections_status'] == "Warning").item():
        print(f"{site} - Connection Status DataFrame:")
        print(dfs['connection_status'])
        print()
        # Iterate through nested connection_status data for each array:
        for idx, row in dfs['connection_status'].iterrows():
            local_array_name = row['array']
            connection_data = row['connection_data']
            print(f"{local_array_name} - Array Connections DataFrame:")
            if connection_data != 'N/A':
                connection_df = pd.DataFrame(connection_data)
                print(f"New DataFrame from ResponseList in [{idx}, {local_array_name} 'connection_data']:")
                print(connection_df)
            else:
                print(f"No connection_data for {local_array_name}")
    """print(f"{site} - ActiveDR Status DataFrame:")
    print(dfs['activeDR_status'])
    print()
    print(f"{site} - ActiveCluster Status DataFrame:")
    print(dfs['activeCluster_status'])
    print()"""




