import numpy as np
import sys
import gc
import cutax
import strax

_, runid = sys.argv
print("Finished importing, now start to load context.")
# Modify below for the strax.storage path
#st = cutax.xenonnt_offline(output_folder="/scratch/midway3/yuanlq/download/daniel_discrepancy5")
st = cutax.xenonnt_offline(output_folder='/dali/lgrandi/yuanlq/collected_outsource/finished_data', 
                           _auto_append_rucio_local=False,
                           _rucio_local_path='/dali/lgrandi/rucio', include_rucio_local=True
                           )
#st = cutax.xenonnt_offline(output_folder='/dali/lgrandi/peaklets_tarballs',
#                           _auto_append_rucio_local=False,
#                           include_rucio_local=False)
# make sure no rucio
#st.storage = [strax.DataDirectory('/dali/lgrandi/manual_process')]
print('Storage:')
print(st.storage)
print('--------------------')
print('raw_records:', st.is_stored(runid, 'raw_records'))
print('lone_hits:', st.is_stored(runid, 'lone_hits'))
print('peak_basics:', st.is_stored(runid, 'peak_basics'))
print('peak_positions_mlp:', st.is_stored(runid, 'peak_positions_mlp'))
print('peak_positions_cnn:', st.is_stored(runid, 'peak_positions_cnn'))
print('peak_positions_gcn:', st.is_stored(runid, 'peak_positions_gcn'))
print('event_basics:', st.is_stored(runid, 'event_basics'))

"""
if st.is_stored(runid, 'cuts_basic'):
    print("Finished processing %s"%(runid))
    exit(0)

if not st.is_stored(runid, 'raw_records'):
    print("We don't have raw_records for %s."%(runid))
    if not (st.is_stored(runid, 'peaklets') and st.is_stored(runid, 'lone_hits')):
        raise ValueError("We don't have raw_records or peaklets or lone_hits for %s. Cannot process"%(runid))
"""
#to_process = ['event_positions_nv','peaklet_classification', 'merged_s2s', 'peak_basics', 'peak_positions_gcn', 'peak_positions_mlp', 'peak_positions_cnn', 'peak_proximity', 'energy_estimates', 'corrected_areas', 'event_positions', 'event_basics', 'event_info', 'distinct_channels', 'event_top_bottom_params', 'event_shadow', 'event_pattern_fit', 'event_ms_naive', 'event_ambience', 'event_area_per_channel']
to_process = ['peaklets', 'merged_s2s', 'peak_basics', 
              'peak_positions_mlp', 'peak_positions_gcn', 'peak_positions_cnn', 
              'event_basics', 'event_area_per_channel', 'event_pattern_fit', 'event_info', 'event_ms_naive', 'event_ambience', 'event_shadow',
              'event_ms_naive', 'event_n_channel', 'event_top_bottom_params',
              'peak_s1_positions_cnn', 'cuts_basic']
#to_process = ['event_ambience', 'event_shadow', 'peak_classification_bayes', 'event_w_bayes_class']

for dt in to_process:
    print('Making %s'%(dt))
    st.make(runid, dt, save=dt)
    print('Done %s'%(dt))
    print('..........')
    gc.collect()

print("Finished processing %s"%(runid))

# Test if we can load the results without problem
try:
    _data = st.get_array(runid, ("peaks", "peak_positions", "peak_basics"), keep_columns=("time"))
    _data = st.get_array(runid, ("cuts_basic", "event_info"), keep_columns=("time"))
    print("Successfully loaded the results.")

    with open("/dali/lgrandi/yuanlq/loadtest/results/events_237_20240529_loadable.txt", "a") as f:
        f.write(f"{runid}\n")
except Exception as e:
    print("Failed to load the results.")
    print(e)

