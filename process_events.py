import strax
import straxen
import numpy as np
import sys
import gc
import cutax

print("Finished importing, now start to load context.")
# Modify below for the strax.storage path
st = cutax.xenonnt_offline(output_folder='/scratch/midway3/yuanlq/download/sr0_bkg_unfinished')
#PATH_TO_REPO = "/project2/lgrandi/cimental/corrections/" 
#st.set_config(
#{"gain_model": f"list-to-array://xedocs://pmt_area_to_pes?as_list=True&detector=tpc&run_id=plugin.run_id&version=v10_test&attr=value&db=local_folder&db__path={PATH_TO_REPO}"}
#)
#st.storage = [strax.DataDirectory('/scratch/midway3/yuanlq/download/sr0_bkg_unfinished')]
_, runid = sys.argv
print("Loaded the context successfully, and the run id to process:", runid)

print("Start to make data")

st.make(runid,"peaklets")
gc.collect()
st.make(runid,"event_basics")
gc.collect()
st.make(runid,"event_info")
gc.collect()
st.make(runid,"event_pattern_fit")
gc.collect()
st.make(runid,"event_area_per_channel", save=("event_area_per_channel"))
gc.collect()
st.make(runid,"event_ms_naive")
gc.collect()
st.make(runid,"cuts_basic")
gc.collect()

if st.is_stored(runid,"cuts_basic"):
    print('Finished all.')
else:
    print('Failed processing...')
