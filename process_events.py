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

to_process = ['event_positions_nv','peaklet_classification', 'merged_s2s', 'peak_basics', 'peak_positions_gcn', 'peak_positions_mlp', 'peak_positions_cnn', 'peak_proximity', 'energy_estimates', 'corrected_areas', 'event_positions', 'event_basics', 'event_info', 'distinct_channels', 'event_top_bottom_params', 'event_shadow', 'event_pattern_fit', 'event_ms_naive', 'event_ambience', 'event_area_per_channel']

for dt in to_process:
    print('Making %s'%(dt))
    st.make(runid, dt)

