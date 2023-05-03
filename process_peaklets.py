import strax
import straxen
import sys
import cutax
import numpy as np

print(straxen.print_versions())
print("Finished importing, now start to load context.")
# Modify below for the strax.storage path
st = cutax.contexts.xenonnt_online(output_folder='/home/yuanlq/scratch-midway2/tc_bug')

_, runid = sys.argv
print("Loaded the context successfully, and the run id to process:", runid)

print("Start to make data")

st.make(runid,"peaklets")

if st.is_stored(runid,"peaklets"):
    print('Finished all.')
else:
    print('Failed processing...')
