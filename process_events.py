import strax
import straxen
import numpy as np
import sys

import numpy as np

print("Finished importing, now start to load context.")
st = straxen.contexts.xenonnt_online(output_folder='/dali/lgrandi/yuanlq/s1_wf_comparison')

_, runid = sys.argv
print("Loaded the context successfully, and the run id to process:", runid)

print("Start to make data")

st.make(runid,"event_info")

if st.is_stored(runid,"event_info"):
    print('Finished all.')
else:
    print('Failed processing...')