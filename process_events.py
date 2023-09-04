import strax
import straxen
import numpy as np
import sys
import cutax

print("Finished importing, now start to load context.")
# Modify below for the strax.storage path
st = cutax.xenonnt_offline(output_folder='/project2/lgrandi/xenonnt/processed')
PATH_TO_REPO = "/project2/lgrandi/cimental/corrections/" 
st.set_config(
{"gain_model": f"list-to-array://xedocs://pmt_area_to_pes?as_list=True&detector=tpc&run_id=plugin.run_id&version=v10_test&attr=value&db=local_folder&db__path={PATH_TO_REPO}"}
)
st.storage.append(strax.DataDirectory('/scratch/midway2/yuanlq/downloads/gain_crisis', readonly=True))
_, runid = sys.argv
print("Loaded the context successfully, and the run id to process:", runid)

print("Start to make data")

st.make(runid,"event_info_double")

if st.is_stored(runid,"event_info"):
    print('Finished all.')
else:
    print('Failed processing...')
