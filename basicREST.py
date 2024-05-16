import purestorage
from purestorage import FlashArray

import requests.packages.urllib3 # type: ignore
requests.packages.urllib3.disable_warnings()

# Initialize Array
array = purestorage.FlashArray("10.235.116.230", api_token="26d6ca06-3496-6f9a-936e-5c88cd6ed359")

# Validate Connection & Output Info
array_info = array.get()
print("FlashArray {} (version {}) REST session established!".format(array_info['array_name'], array_info['version']))