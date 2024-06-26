import strax
import straxen
import numpy as np
import sys
import gc
import cutax
import time
from random import randint


print("Finished importing, now start to load context.")
# Modify below for the strax.storage path
st = cutax.xenonnt_offline()
_, runid = sys.argv
runid = str(runid).zfill(6)

print('Storage:')
print(st.storage)
print('--------------------')
print(runid)
print('raw_records:', st.is_stored(runid, 'raw_records'))
print('peaklets:', st.is_stored(runid, 'peaklets'))
print('lone_hits:', st.is_stored(runid, 'lone_hits'))
print('peak_basics:', st.is_stored(runid, 'peak_basics'))
print('peak_positions_mlp:', st.is_stored(runid, 'peak_positions_mlp'))
print('peak_positions_cnn:', st.is_stored(runid, 'peak_positions_cnn'))
print('peak_positions_gcn:', st.is_stored(runid, 'peak_positions_gcn'))
print('event_basics:', st.is_stored(runid, 'event_basics'))
print('event_info', st.is_stored(runid, 'event_info'))
print('event_pattern_fit', st.is_stored(runid, 'event_pattern_fit'))
print('cuts_basic', st.is_stored(runid, 'cuts_basic'))

to_load = [("event_info", "cuts_basic"), ("peak_positions", "peak_basics")]


try:
    tried = st.get_array(runid, ("event_info", "cuts_basic"), keep_columns=("time"))
    tried = st.get_array(runid, ("peak_positions", "peak_basics"), keep_columns=("time"))
    sleep_time = randint(1, 5)  # Random integer between 1 and 5
    time.sleep(sleep_time)
    with open('/project/lgrandi/yuanlq/loadtest/sr1_bkg_loadable_events.txt', 'a') as f:
        f.write(runid+"\n")
except Exception as e:
    print("Failed", runid, "event_info", "cuts_basic")
    print(e)


print('Done with run %s!'%(runid))
