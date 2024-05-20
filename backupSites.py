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


#############################################
##-----------------------------------------##
##               Run Program               ##
##-----------------------------------------##
#############################################




#def get_arrays_per_site(siteName):



def process_json_config(filePath):
    """
    # Load the JSON file
    with open(filePath, 'r') as file:
        config = json.load(file)

    # Prepare Sites DataFrame
    sitesList = list(config["sites"].keys())
    sitesDF = pd.DataFrame(sitesList, columns='Site')

    print()
    
    # Iterate through the sites
    
    for site, arrays in config["sites"].items():
        print(f"Processing site: {site}")
        
        # Iterate through the arrays within each site
        for array in arrays:
            array_name = array["array"]
            mgmt_ip = array["mgmt_ip"]
            api_token = array["api_token"]
            
            # Perform some actions with the values
            print(f"Array: {array_name}")
            print(f"Management IP: {mgmt_ip}")
            print(f"API Token: {api_token}")
            print("-" * 30)
    """

# Read the config file and transform to JSON data
configJSON = "config.json"
config = prep_config(configJSON)

# Prepare DataFrames for each Site and the Arrays in each site
sitesDF = get_sites(config)
arraysDF = get_arrays(config)

print(sitesDF)

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
    if 'activeDR_status' not in siteArraysDF.columns:
        siteArraysDF['activeDR_status'] = pd.NA
    if 'activeCluster_status' not in siteArraysDF.columns:
        siteArraysDF['activeCluster_status'] = pd.NA

    for index, row in siteArraysDF.iterrows(): # Iterate through arraysDF to collect data for each array in each site

        ### Establish REST API session ###
        mgmtIP = row['mgmt_ip']
        apiToken = row['api_token']

        array, array_info = establish_session(mgmtIP, apiToken)

        # Update siteArraysDF with array_name & purity_version
        siteArraysDF.at[index, 'array_name'] = array_info['array_name']
        siteArraysDF.at[index, 'purity_version'] = array_info['version']
        #### End establish REST API session ###

        ### Get Capacity Information ###
        ### End Capacity Information ###

        ### Get ActiveDR (async) Information ###
        ### End ActiveDR (async) Information ###

        ### Get ActiveCluster (sync) Information ###
        ### End ActiveCluster (sync) Information ###

        print(siteArraysDF)
    
    # Remove array, mgmt_ip and api_token columns from siteArraysDF
    siteArraysDF = siteArraysDF.drop(columns=['array', 'mgmt_ip', 'api_token'])
    print(siteArraysDF)


    #establish_session(siteArraysDF)

#-------------------------------------------#
#                Define Sites               #
#-------------------------------------------#
#sites = ['Salt Lake City', 'Mountain View']
#-------------------------------------------#
#            End Sites Definition           #
#-------------------------------------------#