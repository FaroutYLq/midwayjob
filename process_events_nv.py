import strax
import straxen
import numpy as np
import sys
import cutax
import numpy as np

print("Finished importing, now start to load context.")
# Modify below for the strax.storage path
st = cutax.contexts.xenonnt_online(output_folder='/project2/lgrandi/tutorial_data/')

_, runid = sys.argv
print("Loaded the context successfully, and the run id to process:", runid)

print("Start to make data")

st.make(runid,"events_nv")

if st.is_stored(runid,"event_info"):
    print('Finished all.')
else:
    print('Failed processing...')
