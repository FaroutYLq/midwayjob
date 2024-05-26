import numpy as np
import sys
import cutax
import time
import ast
from random import randint


print("Finished importing, now start to load context.")
# Modify below for the strax.storage path
st = cutax.xenonnt_offline(_auto_append_rucio_local=False, 
                           _rucio_local_path='/dali/lgrandi/rucio', 
                           include_rucio_local=True, 
                           output_folder='/dali/lgrandi/yuanlq/pb')
_, list_dir = sys.argv
data_list = ast.literal_eval(list_dir)

for r in data_list:
    runid = r
    runid = str(runid).zfill(6)

    print('Runid:', runid)
    print('Storage:')
    print(st.storage)
    print('--------------------')
    print(runid)
    dts = ["peaklets", "lone_hits", "merged_s2s", "peaklet_classification", 
        "peak_basics", "peak_positions_mlp", "peak_positions_cnn", "peak_positions_gcn"]

    is_stored = True
    for dt in dts:
        _is_stored = st.is_stored(runid, dt)
        is_stored &= _is_stored
        print(dt+":", _is_stored)

    if is_stored:
        try:
            tried = st.get_array(runid, ("peaks","peak_basics", "peak_positions"), keep_columns=("time"))
            sleep_time = randint(1, 5)  # Random integer between 1 and 5
            time.sleep(sleep_time)
            with open('/dali/lgrandi/yuanlq/loadtest/sr1_rn222_loadable_peaks.txt', 'a') as f:
                f.write(runid+"\n")
        except:
            pass
        
        
    print('Done with run %s!'%(runid))
    print('========================')