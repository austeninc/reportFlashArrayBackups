import purestorage
from purestorage import FlashArray

import pandas as pd

import requests.packages.urllib3 # type: ignore
requests.packages.urllib3.disable_warnings() # Ignore SSL errors due to self-signed certs on Pure appliances

#-------------------------------------------#
#                Define Sites               #
#-------------------------------------------#
sites = ['Salt Lake City', 'Mountain View']
#-------------------------------------------#
#            End Sites Definition           #
#-------------------------------------------#