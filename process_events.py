import strax
import straxen
import numpy as np
import sys
import cutax

print("Finished importing, now start to load context.")
# Modify below for the strax.storage path
st = cutax.xenonnt_online(output_folder='/project/lgrandi/xenonnt/processed')
#PATH_TO_REPO = "/project2/lgrandi/cimental/corrections/" 
#st.set_config(
#{"gain_model": f"list-to-array://xedocs://pmt_area_to_pes?as_list=True&detector=tpc&run_id=plugin.run_id&version=v10_test&attr=value&db=local_folder&db__path={PATH_TO_REPO}"}
#)
#st.storage.append(strax.DataDirectory('/scratch/midway2/yuanlq/downloads/gain_crisis', readonly=True))
_, runid = sys.argv
print("Loaded the context successfully, and the run id to process:", runid)

print("Start to make data")

to_process = ['event_positions_nv','peaklet_classification', 'merged_s2s', 'peak_basics', 'peak_positions_gcn', 'peak_positions_mlp', 'peak_positions_cnn', 'peak_proximity', 'energy_estimates', 'corrected_areas', 'event_positions', 'event_basics', 'event_info', 'distinct_channels', 'event_top_bottom_params', 'event_shadow', 'event_pattern_fit', 'event_ms_naive', 'event_ambience', 'event_area_per_channel']

for dt in to_process:
    print('Making %s'%(dt))
    st.make(runid, dt)

if st.is_stored(runid,"event_info"):
    print('Finished all.')
else:
    print('Failed processing...')
