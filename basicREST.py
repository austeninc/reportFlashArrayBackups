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
    print("---- Replica Links ---------------------\n")
    replicas = array.list_pod_replica_links()
    for r in range(len(replicas)):
        print(replicas[r], "\n")

    replicasDF = pd.DataFrame(replicas)

    replicasDF.rename(columns={'local_pod_name': 'Local Pod',
                               'remote_names': 'Remote Array',
                               'status': 'Status',
                               'remote_pod_name': 'Remote Pod',
                               'recovery_point': 'Recovery Point',
                               'direction': 'Direction',
                               'lag': 'Lag'
                               }, inplace=True)
     
    print(replicasDF)

    replicasOutputDF = update_dataframe(replicasDF)

    # Specify the new column order
    new_order = ['Local Pod', 'Direction', 'Remote Array', 'Remote Pod', 'Status', 'Recovery Point', 'Lag']

    # Reorder the DataFrame columns
    replicasOutputDF = replicasOutputDF[new_order]

    # Sort by Direction
    replicasOutputDF = replicasOutputDF.sort_values(by=['Local Pod', 'Direction'], ascending=True)

    print(replicasOutputDF)

    return(replicasOutputDF)

def update_dataframe(input):

    df = input

    # Replace 'Engineer' with 'Software Engineer' in the 'Occupation' column
    df = df.replace({'replicating': 'Replicating',
                    'outbound': '\N{RIGHTWARDS ARROW}',
                    'inbound': '\N{LEFTWARDS ARROW}'
                    })
    
    # Convert Timestamps to Human Readable
    df['Recovery Point'] = pd.to_datetime(df['Recovery Point'], origin='unix', unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Los_Angeles').to_datetime64()
    #df['Recovery Point'] = pd.to_datetime(df['Recovery Point'], format='%d%M%Y')


    return(df)

    

# Convert DataFrame to HTML
def make_html(dataFrameInput):
    dataFrameHTML = dataFrameInput.to_html(index=False)

    htmlOutput = format_table(dataFrameHTML)

    return(htmlOutput)

# Format any HTML table
def format_table(htmlInput):

    htmlInput = htmlInput.replace('<table border="1" class="dataframe">',
                                      '<table style="border-collapse: collapse; width: 100%; font-family: Arial, sans-serif;">')
    htmlInput = htmlInput.replace('<thead>',
                                            '<thead style="color: white; border-bottom: 5px solid #FE5000;">')
    htmlInput = htmlInput.replace('<th>',
                                            '<th style="color:white; border-bottom: 1px solid #FE5000; text-align: left; padding: 8px;">')
    htmlInput = htmlInput.replace('<td>',
                                            '<td style="color:white; border-bottom: 1px solid #DADADA; text-align: left; padding: 8px;">')
    #htmlInput = htmlInput.replace('<tr>',
    #                                        '<tr style="background-color: #6C6C6C;">')
    htmlInput = htmlInput.replace('<tr>',
                                            '<tr style="color:white; background-color: #1C1C1C;">')

    #print(htmlInput)
    return(htmlInput)

# Write output to HTML file
def write_html(html):
    with open('test_output.html', 'w') as f:
        f.write("<body style=\"background-color: #1C1C1C; padding-top: 2vh; padding-left: 7vw; padding-right: 8vw;\">\n")
        f.writelines(html)
        f.writelines("\n</body>")

################
# Run Program  #
################

# Initialize Array
array = purestorage.FlashArray("10.235.116.230", api_token="26d6ca06-3496-6f9a-936e-5c88cd6ed359")
array_info = array.get()

establish_session()

# Get datafrane from function
dataframe = get_replicaStatus()

# Convert to HTML code from function
## make_html also calls format_table() for styling!!
htmlOutput = make_html(dataframe)

# Write HTML to file
write_html(htmlOutput)