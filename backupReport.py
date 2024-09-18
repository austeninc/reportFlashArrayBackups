import purestorage
from purestorage import FlashArray

import os
import datetime
from datetime import datetime, timezone, timedelta

import pandas as pd
import yaml
import math

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests.packages.urllib3  # type: ignore
requests.packages.urllib3.disable_warnings()  # Ignore SSL errors due to self-signed certs on Pure appliances

################################################
##### UPDATE THESE FOR EMAIL FUNCTIONALITY #####
################################################

smtp_server = 'smtp.dev.purestorage.com'
sender_email = "aclement@purestorage.com"
receiver_email = "aclement@purestorage.com"

################################################
################################################

# Set pandas display options to avoid scientific notation
pd.set_option('display.float_format', '{:,.2f}'.format)

# Get time of report generation
def report_time():
    # Get the current date and time
    pacific = timezone(timedelta(hours=-7))
    now = datetime.now(pacific)
    reportTimeHuman = now.strftime("%Y-%m-%d %H:%M:%S")
    reportTimeFormatted = now.strftime("%Y-%m-%d_%H-%M")
    return reportTimeFormatted, reportTimeHuman

reportTimeFormatted, reportTimeHuman = report_time()
fileName = f"reports/pure_backupReport_{reportTimeFormatted}.html"

# Report Header
head_bar = f"<table style=\"vertical-align: center; font-size: large; width: 100%; height: 75px; table-layout: auto;\"><tbody style=\"vertical-align: center; color: white; font-family: 'Inter', 'San Francisco', Helvetica, Arial, sans-serif;\"><tr style=\"vertical-align: center;\"><td style=\"vertical-align: center; width: 23px;\"><svg width=\"55\" height=\"49.52368\" viewBox=\"0 0 960 864.41333\"><g id=\"g8\" transform=\"matrix(1.3333333,0,0,-1.3333333,0,864.41333)\"><g id=\"g10\" transform=\"scale(0.1)\"><path d=\"M 3852.7,0 H 2261.04 c -328.55,0 -634.8,176.75 -799,460.973 L 122.914,2780.13 c -163.8632,284.64 -163.8632,637.98 -0.141,922.28 l 1339.267,2319.3 c 164.07,284.64 470.31,461.38 799,461.38 h 2677.94 c 328.41,0 634.52,-176.74 798.93,-461.38 L 7076.9,3702.61 c 164.13,-284.29 164.13,-637.84 0,-922.48 L 6730.62,2228.16 C 6569.39,1948.72 6263.15,1772.12 5934.46,1772.12 H 4340.61 l 871.37,1470.5 -805.99,1395.29 H 2793.61 L 1987.27,3241.58 3852.7,0\" style=\"fill:#f0401e;fill-opacity:1;fill-rule:nonzero;stroke:none\" id=\"path12\" /></g></g></svg></td><td style=\"padding-left: 10px; vertical-align: center; width: 85px; min-width: 85px;\"><h1 style=\"font-weight:800;\">FlashArray</h1></td><td style=\"padding-left: 8px; vertical-align: center; width: 325px;\"><h1 style=\"font-weight:300;\">Replication Status</h1></td><td style=\"padding-left: 10px; padding-top: .1em; vertical-align: center;\"><p style=\"font-size: x-small; margin: 0 0 2px 0; font-weight: 700; color:#FE5000;\">Report Generated:</p><p style=\"font-size: small; margin: 0 0 0 0; font-weight: 300;\">{reportTimeHuman} Pacific (UTC-0700)</p></td><td style=\"min-width: 35px;\"></td><td style=\"padding-top: .6em; text-align: right;\"><svg width=\"138.2185\" height=\"32\" viewBox=\"0 0 10666.667 2469.52\"><g id=\"g8\"transform=\"matrix(1.3333333,0,0,-1.3333333,0,2469.52)\"><g id=\"g10\"transform=\"scale(0.1)\"><path d=\"m 80000,18521.3 v 0.1 H 56201.9 c -644,0 -1186.2,-184.2 -1630.2,-563.3 444,379.1 986.2,563.2 1630.2,563.2 H 80000\"style=\"fill:#ffffff;fill-opacity:1;fill-rule:nonzero;stroke:none\"id=\"path12\" /><path d=\"m 54571.7,17958.1 c -345.4,-295 -631.4,-707.8 -859.8,-1243.7 L 47021.9,953 v 0 l 6690,15761.3 c 228.4,536 514.4,948.8 859.8,1243.8\"style=\"fill:#ffffff;fill-opacity:1;fill-rule:nonzero;stroke:none\"id=\"path14\" /><path d=\"M 72253.6,3210.1 C 71838.4,2235.6 71319.2,2101.2 70820.1,2101.2 H 48756.7 l 6011.6,14163 c 415.3,974.6 934.4,1108.9 1433.6,1108.9 H 78265.2 Z M 56201.9,18521.3 c -644,0 -1186.2,-184.1 -1630.2,-563.2 -345.4,-295 -631.4,-707.8 -859.8,-1243.8 L 47021.9,953 v 0 h 23798.2 c 858.6,0 1536.3,327.4 2041.6,1007.9 42.1,56.7 83,115.8 122.8,177.5 119.2,184.8 227.6,391.9 325.5,621.7 l 6690,15761.2 v 0 H 56201.9\"style=\"fill:#ffffff;fill-opacity:1;fill-rule:nonzero;stroke:none\"id=\"path16\" /><path d=\"M 5237.09,17405.1 H 1223.73 v -6137.8 h 4013.36 c 1888.33,0 3154.6,1266 3154.6,3068.9 0,1803 -1266.27,3068.9 -3154.6,3068.9 z M 0.00390625,18521.3 H 5365.94 c 2747.08,0 4313.6,-1931.5 4313.6,-4185.1 0,-2253.6 -1588.09,-4185 -4313.6,-4185 H 1223.73 V 4206.3 H 0.00390625 v 14315\"style=\"fill:#ffffff;fill-opacity:1;fill-rule:nonzero;stroke:none\"id=\"path18\" /><path d=\"m 18612.4,5730.3 c -858.8,-944.5 -2232.3,-1781.7 -3734.7,-1781.7 -2124.5,0 -3261.9,987.7 -3261.9,3326.6 v 7297.1 h 1115.9 V 7532.9 c 0,-2017.1 1008.5,-2575.7 2510.9,-2575.7 1330.4,0 2683,794.1 3369.8,1717 v 7898.1 h 1115.8 v -10366 h -1115.8 v 1524\"style=\"fill:#ffffff;fill-opacity:1;fill-rule:nonzero;stroke:none\"id=\"path20\" /><path d=\"m 22608.4,14572.3 h 1115.8 V 12791 c 879.8,1159 2060.3,1995.9 3498,1995.9 v -1223.5 c -193,43.2 -342.8,43.2 -557.9,43.2 -1051.2,0 -2467.9,-944.5 -2940.1,-1845.8 V 4206.3 h -1115.8 v 10366\"style=\"fill:#ffffff;fill-opacity:1;fill-rule:nonzero;stroke:none\"id=\"path22\" /><path d=\"m 28933.7,14572.3 h 1115.9 v -10366 h -1115.9 z m -300.8,2575.4 c 0,472.3 408.1,858.5 858.8,858.5 472.2,0 858.1,-386.2 858.1,-858.5 0,-472.2 -385.9,-858.4 -858.1,-858.4 -450.7,0 -858.8,386.2 -858.8,858.4\"style=\"fill:#ffffff;fill-opacity:1;fill-rule:nonzero;stroke:none\"id=\"path24\" /><path d=\"M 33353.4,6202.5 V 13585 h -1716.9 v 987.3 h 1716.9 v 2832.8 h 1137.4 v -2832.8 h 2103.5 V 13585 H 34490.8 V 6373.9 c 0,-837.2 343.4,-1416.7 1073.4,-1416.7 493.2,0 922.8,236.1 1159,493.8 l 429,-858.1 c -408.1,-386.6 -901.3,-644.3 -1760,-644.3 -1373.6,0 -2038.8,837.2 -2038.8,2253.9\"style=\"fill:#ffffff;fill-opacity:1;fill-rule:nonzero;stroke:none\"id=\"path26\" /><path d=\"M 47677.9,14572.3 42312.6,1931.5 C 41754.7,622.004 40875,0 39651.2,0 c -343.4,0 -815.6,64.1016 -1094.3,149.801 l 193,1030.199 c 236.1,-107.3 643.6,-171.4 901.3,-171.4 686.8,0 1159,279.2 1567.1,1266.3 l 815,1845.7 -4377.7,10451.7 h 1244.7 l 3777.3,-9056.5 3734.1,9056.5 h 1266.2\"style=\"fill:#ffffff;fill-opacity:1;fill-rule:nonzero;stroke:none\"id=\"path28\" /><path d=\"m 53498.9,4889.3 4145.7,9788.8 h 6707.2 L 63722,13166.4 h -4990.2 l -1070.3,-2524.2 h 4887.4 L 61904.2,9130.78 H 57017 L 55216.1,4889.3 h -1717.2\"style=\"fill:#ffffff;fill-opacity:1;fill-rule:nonzero;stroke:none\"id=\"path30\" /><path d=\"M 68817.9,12961 65081.4,8294.1 h 3522.4 z m -450.6,-8071.7 76.2,1893.2 h -4491 L 62437.9,4889.3 h -2054.8 l 8005.6,9788.8 h 2142.8 l -300.3,-9788.8 h -1863.9\"style=\"fill:#ffffff;fill-opacity:1;fill-rule:nonzero;stroke:none\"id=\"path32\" /></g></g></svg></td></tr></tbody></table>"

def read_yaml(filePath):
    with open(filePath, 'r') as file:
        yamlData = yaml.safe_load(file)
    return yamlData

def create_nested_dataframes(arrays):
    capacity_data = [{'array': array['array'],
                      'percent_full': array.get('percent_full', 'N/A'),
                      'drr': array.get('drr', 'N/A'),
                      'space_used': array.get('space_used', 'N/A'),
                      'space_remaining': array.get('space_remaining', 'N/A'),
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

def create_nested_connection_df(array_name):
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

def create_nested_activeDR_df(array_name):
    additional_data = [{'subfield1': f'subvalue1 for {array_name}', 'subfield2': f'subvalue2 for {array_name}'}]
    additional_activeDR_df = pd.DataFrame(additional_data)
    return additional_activeDR_df

def create_nested_activeCluster_df(array_name):
    additional_data = [{'subfield1': f'subvalue1 for {array_name}', 'subfield2': f'subvalue2 for {array_name}'}]
    additional_activeCluster_df = pd.DataFrame(additional_data)
    return additional_activeCluster_df

def establish_session(mgmtIP, apiToken):
    array = purestorage.FlashArray(mgmtIP, api_token=apiToken)
    array_info = array.get()
    return array, array_info

def convert_capacity(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   size_output = f"{s} {size_name[i]}"
   return size_output

def capacity_calculations(array_capacity_df):
    array_consumed = array_capacity_df.at[0, 'total']
    array_usable = array_capacity_df.at[0, 'capacity']
    array_percent_full = array_consumed / array_usable * 100
    array_space_remaining = array_usable - array_consumed
    array_consumed = convert_capacity(array_consumed)
    array_usable = convert_capacity(array_usable)
    array_space_remaining = convert_capacity(array_space_remaining)
    array_drr = array_capacity_df.at[0, 'data_reduction']
    return array_consumed, array_usable, array_percent_full, array_drr, array_space_remaining

def update_capacity(arrays_df, capacity_df):
    for index, row in arrays_df.iterrows():
        # Connect
        mgmtIP = row['mgmt_ip']
        apiToken = row['api_token']
        array, array_info = establish_session(mgmtIP, apiToken)
        # Add local_version
        local_version = array_info['version']
        capacity_df['local_version'] = local_version
        # Get Capacity Information
        array_capacity = array.get(space=True)
        array_capacity_df = pd.DataFrame(array_capacity)
        array_consumed, array_usable, array_percent_full, array_drr, array_space_remaining = capacity_calculations(array_capacity_df)
        array_percent_full = str(round(array_percent_full, 2))
        array_drr = str(round(array_drr, 1))
        capacity_df.at[index, 'percent_full'] = f"{array_percent_full}%"
        capacity_df.at[index, 'drr'] = f"{array_drr}:1"
        capacity_df.at[index, 'space_used'] = array_consumed
        capacity_df.at[index, 'total_usable'] = array_usable
        capacity_df.at[index, 'space_remaining'] = array_space_remaining
    return capacity_df

def update_connections(arrays_df, connections_df):
    for index, row in arrays_df.iterrows():
        # Connect
        mgmtIP = row['mgmt_ip']
        apiToken = row['api_token']
        array, array_info = establish_session(mgmtIP, apiToken)
        # Populate Local Purity Version
        local_version = array_info['version']
        connections_df['local_version'] = local_version
        # Get Array Connections
        array_connections_data = array.list_array_connections()
        array_connections_df = pd.DataFrame(array_connections_data)
        if array_connections_df.empty:
            connections_df.at[index, 'connection_status'] = array_connections_data
        elif (array_connections_df['status'] == 'connected').all():
            connections_df.at[index, 'connection_status'] = "Okay"
            connections_df.at[index, 'connection_data'] = array_connections_data
        else:
            connections_df.at[index, 'connection_status'] = "Warning"
            connections_df.at[index, 'connection_data'] = array_connections_data

def update_activeDR(arrays_df, activeDR_df):
    for index, row in arrays_df.iterrows():
        # Connect
        mgmtIP = row['mgmt_ip']
        apiToken = row['api_token']
        array, array_info = establish_session(mgmtIP, apiToken)
        # Populate Local Purity Version
        local_version = array_info['version']
        activeDR_df['local_version'] = local_version
        # Get ActiveDR Data
        array_activeDR_data = array.list_pod_replica_links()
        array_activeDR_df = pd.DataFrame(array_activeDR_data)
        if array_activeDR_df.empty:
            activeDR_df.at[index, 'activeDR_data'] = "N/A"
        elif (array_activeDR_df['status'] == 'replicating').all():
            activeDR_df.at[index, 'activeDR_status'] = "Okay"
            activeDR_df.at[index, 'activeDR_data'] = array_activeDR_data
        else:
            activeDR_df.at[index, 'activeDR_status'] = "Warning"
            activeDR_df.at[index, 'activeDR_data'] = array_activeDR_data

def update_activeCluster(arrays_df, activeCluster_df):
    for index, row in arrays_df.iterrows():
        # Connect
        mgmtIP = row['mgmt_ip']
        apiToken = row['api_token']
        array, array_info = establish_session(mgmtIP, apiToken)
        # Populate Local Purity Version
        local_version = array_info['version']
        activeCluster_df['local_version'] = local_version
        # Get Pods Data
        array_podsAC_data = array.list_pods()
        # Filter Pods for ActiveCluster members
        array_activeCluster_data = [entry for entry in array_podsAC_data if len(entry['arrays']) > 1]
        array_activeCluster_df = pd.DataFrame(array_activeCluster_data)
        for idx, line in array_activeCluster_df.iterrows():
            activeCluster_members_data = line['arrays']
            activeCluster_members_df = pd.DataFrame(activeCluster_members_data)
            if activeCluster_members_df.empty:
                activeCluster_df.at[index, 'activeCluster_data'] = "N/A"
            elif (activeCluster_members_df['status'] == 'online').all():
                activeCluster_df.at[index, 'activeCluster_status'] = "Okay"
                activeCluster_df.at[index, 'activeCluster_data'] = array_podsAC_data
            else:
                activeCluster_df.at[index, 'activeCluster_status'] = "Warning"
                activeCluster_df.at[index, 'activeCluster_data'] = array_podsAC_data

def update_site_summary_status(site, summary_df, site_status_column, nested_status_column, nested_status_df):
    status = "Okay"
    if (nested_status_df[nested_status_column] == "Warning").any():
        status = "Warning"
    summary_df.loc[summary_df['site'] == site, site_status_column] = status
    return status
def remove_null_rows(nested_data_column, nested_df):
    # drop all rows that contain 'N/A'
    nested_clean_df = nested_df.drop(nested_df[nested_df[nested_data_column] == 'N/A'].index)
    return nested_clean_df

def explode_data(nested_data_column, nested_status_column, nested_df):
    # Explode the nested data column
    exploded_data = nested_df.explode(nested_data_column).reset_index(drop=True)
    # Normalize the exploded data to flatten the nested JSON objects
    normalized_data = pd.json_normalize(exploded_data[nested_data_column])
    # Clean up column names to clearly state "remote_array"
    normalized_data = normalized_data.rename(columns={'array_name': 'remote_array', 'version': 'remote_version'})
    # Drop the nested columns from the exploded DataFrame
    exploded_data = exploded_data.drop(columns=[nested_data_column, nested_status_column])
    # Concatenate the normalized data with the exploded data
    merged_nested_df = pd.concat([normalized_data, exploded_data], axis=1)

    return merged_nested_df

def explode_activeCluster_arrays(formatted_activeCluster_df):
    # Explode the nested data column
    exploded_arrays = formatted_activeCluster_df.explode('arrays').reset_index(drop=True)
    # Normalize the exploded data
    normalized_arrays = pd.json_normalize(exploded_arrays['arrays'])
    # Drop the nested columns from the exploded dataframe
    exploded_arrays = exploded_arrays.drop(columns=['arrays'])
    # Concatenate the normalized data with the exploded data
    merged_df = pd.concat([normalized_arrays, exploded_arrays], axis=1)
    # Clean up duplicate array names
    cols_to_check = ['local_array', 'local_version', 'pod_name', 'promotion_status']
    merged_df.loc[:, cols_to_check] = merged_df.loc[:, cols_to_check].mask(merged_df.loc[:, cols_to_check].duplicated(), '')

    return merged_df

def format_dataframe(input_type, input_df):
    df = input_df

    # Perform Actions based on Input Type
    if input_type == "capacity":
        # Re-Order Columns:
        new_order = ['array', 'local_version', 'drr', 'percent_full', 'space_used', 'space_remaining', 'total_usable']
        df = df[new_order]
        # Clean Up Columns
        df = df.rename(columns={'local_version': 'version'})

    if input_type == "connection":
        # Re-Order Columns
        new_order = ['array', 'local_version', 'status', 'remote_array', 'remote_version', 'type', 'throttled', 'management_address', 'replication_address', 'id']
        df = df[new_order]
        # Clean Up Columns
        df = df.drop(columns=['id']).reset_index(drop=True)
        df = df.rename(columns={'array': 'local_array'})
        # Sort by Direction
        df = df.sort_values(by=['status', 'remote_array'], ascending=False)

    if input_type == "activeDR":
        # Clean Up Columns
        df = df.drop(columns=['local_version']).reset_index(drop=True)
        df = df.rename(columns={'array': 'local_array'})
        # Re-Order Columns
        new_order = ['local_array', 'local_pod_name', 'direction', 'remote_names', 'remote_pod_name', 'status', 'recovery_point', 'lag']
        df = df[new_order]
        # Sort by Direction
        df = df.sort_values(by=['local_pod_name', 'direction'], ascending=True)

    if input_type == "activeCluster":
        # Clean Up Columns
        df = df.drop(columns=['requested_promotion_state', 'link_target_count', 'link_source_count', 'source']).reset_index(drop=True)
        df = df.rename(columns={'array': 'local_array', 'name': 'pod_name'})

        df = explode_activeCluster_arrays(df)

        # Re-Order Columns
        new_order = ['local_array', 'local_version', 'pod_name', 'promotion_status', 'name', 'status', 'mediator_status', 'pre_elected', 'frozen_at']
        df = df[new_order]
        # Drop More Columns
        df = df.drop(columns=['pre_elected', 'local_version'])

    # Convert UNIX Timestamps to Human Readable
    if 'recovery_point' in df:
        df['recovery_point'] = pd.to_datetime(df['recovery_point'], unit='ms', utc=True)    # Assume UTC source
        df['recovery_point'] = df['recovery_point'].dt.tz_convert('America/Los_Angeles')    # Assume convert to Pacific time
        df['recovery_point'] = df['recovery_point'].dt.strftime('%Y-%m-%d %H:%M:%S %Z')     # Make human readable
    if 'frozen_at' in df:
        df['frozen_at'] = pd.to_datetime(df['frozen_at'], unit='ms', utc=True)    # Assume UTC source
        df['frozen_at'] = df['frozen_at'].dt.tz_convert('America/Los_Angeles')    # Assume convert to Pacific time
        df['frozen_at'] = df['frozen_at'].dt.strftime('%Y-%m-%d %H:%M:%S %Z')     # Make human readable

    # Convert Lag Time ms to seconds
    if 'lag' in df:
        df['lag'] = pd.to_numeric(df['lag']) / 1000

    # Replace Column Titles with Human-Readable (Dictionary)
    df.rename(columns={'site': 'Site Name',
                       'site_connections_status': 'Array Connections',
                       'site_activeDR_status': 'ActiveDR (async) Status',
                       'site_activeCluster_status': 'ActiveCluster (sync) Status',
                       'version': 'Purity Version',
                       'local_version': 'Local Purity Version',
                       'remote_version': 'Remote Purity Version',
                     'throttled': 'Throttled',
                     'status': 'Status',
                     'management_address': 'Management IP',
                     'id': 'Array ID',
                     'array_name': 'Array',
                     'array': 'Array',
                     'local_array': 'Local Array',
                     'name': 'Member Arrays',
                     'replication_address': 'Replication IP',
                     'type': 'Replication Type',
                     'local_pod_name': 'Local Pod',
                     'remote_names': 'Remote Array',
                     'remote_array': 'Remote Array',
                     'remote_pod_name': 'Remote Pod',
                     'recovery_point': 'Recovery Point',
                     'direction': 'Direction',
                     'lag': 'Lag (seconds)',
                     'frozen_at': 'Frozen Since',
                     'pod_name': 'Pod',
                     'pod_source': 'Source',
                     'promotion_status': 'Promotion Status',
                     'mediator_status': 'Mediator Status',
                     'alert_id': 'Alert ID',
                     'current_severity': 'Severity',
                     'opened': 'Opened',
                     'alert_code': 'Alert Code',
                     'component_type': 'Component',
                     'event': 'Event',
                     'alert_details': 'Details',
                     'percent_full': 'Percent Full',
                     'space_used': 'Used',
                     'space_remaining': 'Available',
                     'total_usable': 'Total Usable',
                     'drr': 'DRR'
                    }, inplace=True)
    
    # Replace contents of cells in columns
    df = df.replace({'replicating': '\N{Large Green Circle} Replicating',
                     'Okay': '\N{Large Green Circle} Okay',
                    'outbound': '\N{RIGHTWARDS ARROW}',
                    'inbound': '\N{LEFTWARDS ARROW}',
                    'Warning': '\N{Large Yellow Circle} Warning',
                    'paused': '\N{Large Yellow Circle} Paused',
                    'unhealthy': '\N{Large Red Circle} Unhealthy',
                    'online': '\N{Large Green Circle} online',
                    'unknown': '\N{Heavy Large Circle} unknown',
                    'offline': '\N{Large Red Circle} offline',
                    'flummoxed': '\N{Large Yellow Circle} flummoxed',
                    'unreachable': '\N{Large Red Circle} unreachable',
                    'resyncing': '\N{Large Blue Circle} resyncing'
                    })
    #print(df)
    return df

#-------------------------------------------#
#            Prepare HTML Output            #
#-------------------------------------------#
# Convert DataFrame to HTML
def make_html(heading, headingStyle, dataframe):
    dataFrameHTML = dataframe.to_html(index=False)
    
    if headingStyle == 1:
        headingHTML = "<h1>" + heading + "</h1>\n\n"
    elif headingStyle == 2:
        headingHTML = "<h2>" + heading + "</h2>\n\n"
    else:
        headingHTML = "<h3>" + heading + "</h3>\n\n"
    
    heading, htmlOutput = format_table(headingHTML, headingStyle, dataFrameHTML)

    write_html(heading, htmlOutput)

    return(heading, htmlOutput)

# Format any HTML table
def format_table(heading, headingStyle, htmlInput):
    if headingStyle == 1:
        headingHTML = heading.replace('<h1>',
                                        '\n<h1 style="color: white; width: 100%; font-family: \'Inter\', \'San Francisco\', Helvetica, Arial, sans-serif; font-size: x-large;">')
    elif headingStyle == 2:
        headingHTML = heading.replace('<h2>',
                                        '\n<h2 style="color: white; width: 100%; font-family: \'Inter\', \'San Francisco\', Helvetica, Arial, sans-serif; font-size: large;">')
    else:
        headingHTML = heading.replace('<h3>',
                                        '\n<h3 style="color: white; width: 100%; font-family: \'Inter\', \'San Francisco\', Helvetica, Arial, sans-serif; font-weight: 300; font-size: large; margin-top: -10px; padding-bottom: 10px;">')

    html = htmlInput.replace('<table border="1" class="dataframe">',
                                '<table style="border-collapse: collapse; width: 100%; font-family: \'Inter\', \'San Francisco\', Helvetica, Arial, sans-serif; font-size: medium; table-layout: auto; min-width: 700px; max-width: 4096px;">')
    html = html.replace('<thead>',
                            '<thead style="color: white; border-bottom: 3px solid #FE5000;">')
    html = html.replace('<th>',
                            '<th style="color:white; border-bottom: 1px solid #FE5000; text-align: left; padding: 0px 0px 6px 3px;">')
    html = html.replace('<td>',
                            '<td style="color:white; border-bottom: 1px solid #474747; text-align: left; padding: 6px 0px 6px 3px;">')
    #html = html.replace('<tr>',
    #                        '<tr style="background-color: #6C6C6C;">')
    html = html.replace('<tr>',
                            '<tr style="color:white; background-color: #0F0F0F;">')
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
    with open(fileName, 'w') as f:
        f.write("<!DOCTYPE html>")
        f.write("<body style=\"background-color: #0F0F0F; padding-top: 2vh; padding-left: 7vw; padding-right: 8vw;\">\n")
        #f.write("<img src='assets/pstg_logo_darkMode.svg' width=250 /><br /><br />")
        f.write(head_bar)

## Add tables to HTML
def write_html(title, html):
    with open(fileName, 'a') as f:
        #f.writelines('</br>')
        f.writelines(title)
        f.writelines(html)

## Quickly add break <br /> tag
def insert_space_html():
    with open(fileName, 'a') as f:
        f.writelines("\n<br />")

def insert_divider_html():
    with open(fileName, 'a') as f:
        f.writelines("\n<hr style=\"height:1px;border-width:0;width:31%;color:#FE5000;background-color:#FFFFFF\">")
        #f.writelines("\n<hr style=\"height:2px;border-width:0;width:49%;color:#FE5000;background-color:#FE5000\">")
        #f.writelines("\n<hr style=\"height:1px;border-width:0;width:31%;color:#FE5000;background-color:#FFFFFF\">")

## Close the file with ending body tag
def end_html_body():
    with open(fileName, 'a') as f:
        f.writelines("\n<br /><br />")
        f.writelines("\n</body>")
#-------------------------------------------#
#          Done Writing HTML File           #
#-------------------------------------------#

#-------------------------------------------#
#          Send Report via Email            #
#-------------------------------------------#
def email_prepare():
    email_subject = f"FlashArray Replication Report - {reportTimeHuman} PT"
    #print(email_subject)
    #print(fileName)
    with open(fileName, 'r', encoding='utf-8') as reportFile:
        html_content = reportFile.read()
    email_body = html_content
    return email_subject, email_body

def send_email(sender_email, receiver_email):
    subject, body = email_prepare()

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = receiver_email

    part = MIMEText(body, 'html')
    msg.attach(part)

    # Send via insecure SMTP server
    try:
        with smtplib.SMTP(smtp_server, 25) as server:
            server.sendmail(sender_email, receiver_email, msg.as_string())
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")
#-------------------------------------------#
#             Done with Email               #
#-------------------------------------------#

#-------------------------------------------#
#           Clean Up Old Reports            #
#-------------------------------------------#
def clean_up():
    report_retention_days = 14
    reports_directory = 'reports/'

    current_time = datetime.now()

    for reportname in os.listdir(reports_directory):
        report_path = os.path.join(reports_directory, reportname)

        if os.path.isfile(report_path):
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(report_path))
            if (current_time - file_mod_time).days > report_retention_days:
                os.remove(report_path)
                print(f"Deleted {report_path} (older than {report_retention_days} days)")
#-------------------------------------------#
#             Done Cleaning Up              #
#-------------------------------------------#

def main():
    configYAML = "config.yml"
    yamlData = read_yaml(configYAML)

    siteSummary = pd.DataFrame({'site': list(yamlData['sites'].keys())})

    site_arrays_dfs = {}
    nested_site_dfs = {}

    for site, arrays in yamlData['sites'].items():
        arrays_df = pd.DataFrame(arrays)
        site_arrays_dfs[site] = arrays_df
        capacity_df, connection_df, activeDR_df, activeCluster_df = create_nested_dataframes(arrays)
        nested_site_dfs[site] = {
            'arrays': arrays_df,
            'capacity': capacity_df,
            'connection_status': connection_df,
            'activeDR_status': activeDR_df,
            'activeCluster_status': activeCluster_df
        }

    for site, dfs in nested_site_dfs.items():
        update_capacity(dfs['arrays'], dfs['capacity'])
        update_connections(dfs['arrays'], dfs['connection_status'])
        update_activeDR(dfs['arrays'], dfs['activeDR_status'])
        update_activeCluster(dfs['arrays'], dfs['activeCluster_status'])

    siteSummary['site_connections_status'] = "N/A"
    siteSummary['site_activeDR_status'] = "N/A"
    siteSummary['site_activeCluster_status'] = "N/A"

    for site, dfs in nested_site_dfs.items():
        siteConnectionStatus = update_site_summary_status(site, siteSummary, 'site_connections_status', 'connection_status', dfs['connection_status'])
        siteActiveDRStatus = update_site_summary_status(site, siteSummary, 'site_activeDR_status', 'activeDR_status', dfs['activeDR_status'])
        siteActiveClusterStatus = update_site_summary_status(site, siteSummary, 'site_activeCluster_status', 'activeCluster_status',dfs['activeCluster_status'])

    # Initialize HTML file
    start_html_body()

    # Output Summary
    siteSummary_copy = siteSummary.copy()
    siteSummary_copy = format_dataframe('summary', siteSummary_copy)
    #print("Summary:")
    #print(siteSummary_copy)

    heading = "Summary"
    make_html(heading, 1, siteSummary_copy)
    insert_space_html()

    for site, dfs in nested_site_dfs.items():
        site_capacity_df = update_capacity(dfs['arrays'], dfs['capacity'])
        site_capacity_df = format_dataframe('capacity', site_capacity_df)
        #print(f"{site} - Array Capacity\n")
        #print(site_capacity_df)
        heading = f"{site} &mdash; Arrays"
        make_html(heading, 2, site_capacity_df)
        insert_space_html()

    for site, dfs in nested_site_dfs.items():
        # Get statuses
        siteConnectionStatus = update_site_summary_status(site, siteSummary, 'site_connections_status', 'connection_status', dfs['connection_status'])
        siteActiveDRStatus = update_site_summary_status(site, siteSummary, 'site_activeDR_status', 'activeDR_status', dfs['activeDR_status'])
        siteActiveClusterStatus = update_site_summary_status(site, siteSummary, 'site_activeCluster_status', 'activeCluster_status',dfs['activeCluster_status'])

        # Clean up dataframes
        clean_connection_df = remove_null_rows('connection_data', dfs['connection_status'])
        clean_activeDR_df = remove_null_rows('activeDR_data', dfs['activeDR_status'])
        clean_activeCluster_df = remove_null_rows('activeCluster_data', dfs['activeCluster_status'])

        # Explode data contents
        connection_detail_df = explode_data('connection_data', 'connection_status', clean_connection_df)
        activeDR_detail_df = explode_data('activeDR_data', 'activeDR_status', clean_activeDR_df)
        activeCluster_detail_df = explode_data('activeCluster_data', 'activeCluster_status', clean_activeCluster_df)

        # If site is flagged for issues, produce Site Problem Report header
        if (siteConnectionStatus == "Warning") | (siteActiveDRStatus == "Warning") | (siteActiveClusterStatus == "Warning"):
            insert_space_html()
            insert_divider_html()
            heading = f"Problem Report &mdash; {site}"
            headingHTML = "<br /><h1 style=\"color: white; width: 100%; font-family: \'Inter\', \'San Francisco\', Helvetica, Arial, sans-serif; font-size: x-large;\">" + heading + "</h1>"
            html = ""
            write_html(headingHTML, html)

        if connection_detail_df.empty == False:
            connection_report_df = format_dataframe('connection', connection_detail_df)
        if activeDR_detail_df.empty == False:
            activeDR_report_df = format_dataframe('activeDR', activeDR_detail_df)
        if activeCluster_detail_df.empty == False:
            activeCluster_report_df = format_dataframe('activeCluster', activeCluster_detail_df)

        if siteConnectionStatus == "Warning":
            #print(f"\n\nReport: {site} Connected Arrays:\n")
            #print(connection_report_df)
            heading = f"Warning: {site} &mdash; Array Connections"
            make_html(heading, 3, connection_report_df)
        if siteActiveDRStatus == "Warning":
            #print(f"\n\nReport: {site} ActiveDR (async) Status:\n")
            #print(activeDR_report_df)
            heading = f"Warning: {site} &mdash; ActiveDR (async) Status"
            make_html(heading, 3, activeDR_report_df)
        if siteActiveClusterStatus == "Warning":
            #print(f"\n\nReport: {site} ActiveCluster (sync) Status:\n")
            #print(activeCluster_report_df)
            heading = f"Warning: {site} &mdash; ActiveCluster (sync) Status"
            make_html(heading, 3, activeCluster_report_df)

    # Complete the HTML file
    end_html_body()
    # Send via email
    send_email(sender_email, receiver_email)
    # Clean up old report files
    clean_up()

if __name__ == "__main__":
    main()