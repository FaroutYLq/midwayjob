import strax
import straxen
import numpy as np
import sys
import gc
import cutax

print("Finished importing, now start to load context.")
# Modify below for the strax.storage path
st = cutax.xenonnt_online(output_folder='/project/lgrandi/xenonnt/processed')
_, runid = sys.argv

to_process = ['cuts_basic']

for dt in to_process:
    print('Making %s'%(dt))
    st.make(runid, dt)

