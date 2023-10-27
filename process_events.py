import strax
import straxen
import numpy as np
import sys
import gc
import cutax

output_dir = '/scratch/midway3/yuanlq/download/sr0_rn220_unfinished'
print("Finished importing, now start to load context.")
# Modify below for the strax.storage path
st = cutax.xenonnt_offline(output_folder=output_dir)
#PATH_TO_REPO = "/project2/lgrandi/cimental/corrections/" 
#st.set_config(
#{"gain_model": f"list-to-array://xedocs://pmt_area_to_pes?as_list=True&detector=tpc&run_id=plugin.run_id&version=v10_test&attr=value&db=local_folder&db__path={PATH_TO_REPO}"}
#)
_, runid = sys.argv
try:
    st.copy_to_frontend(runid, 'raw_records_aqmon')
except:
    print("raw_records_aqmon already existed!")
st.storage = [strax.DataDirectory(output_dir)]
print("Have raw_records:", st.is_stored(runid,'raw_records'))
print("Loaded the context successfully, and the run id to process:", runid)

print("Start to make data")

st.make(runid,"peaklets")
gc.collect()
print("peaklets done")

st.make(runid,"event_basics")
gc.collect()
print("event_basics done")

st.make(runid,"event_info")
gc.collect()
print("event_info done")

st.make(runid,"event_shadow")
gc.collect()
print("event_shadow done")

st.make(runid,"event_ambience")
gc.collect()
print("event_ambience done")

st.make(runid,"event_pattern_fit")
gc.collect()
print("event_pattern_fit done")

st.make(runid,"event_area_per_channel", save=("event_area_per_channel"))
gc.collect()
print("event_area_per_channel done")

st.make(runid,"event_ms_naive")
gc.collect()
print("event_ms_naive done")

st.make(runid,"cuts_basic")
gc.collect()
print("cuts_basic done")

if st.is_stored(runid,"cuts_basic"):
    print('Finished all.')
else:
    print('Failed processing...')
