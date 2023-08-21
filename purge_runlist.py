import numpy as np
import sys
import cutax
from tqdm import tqdm

osg_all_dtypes = ['lone_hits', 'peaklets', 'merged_s2s', 'peaklet_classification', 'peak_basics', 
                  'distinct_channels', 'event_basics', 'corrected_areas', 'energy_estimates', 'event_info',
                  'event_pattern_fit', 'event_positions', 'peak_positions_gcn', 'peak_positions_cnn', 
                  'peak_proximity', 'peak_positions_mlp']

_, directory, index = sys.argv
print("The directory to process:", directory)

st = cutax.xenonnt_offline()

with open(directory, 'r') as f:
    runids = [int(line.strip()) for line in f]


corrupted_dids = []
for run in tqdm(runids):
    run = str(run).zfill(6)
    for dtype in osg_all_dtypes:
        if st.is_stored(run, dtype):
            try:
                loaded = st.get_array(run, dtype, keep_columns=('time'),progress_bar=False)
            except:
                corrupted_dids.append('xnt_'+run+':'+dtype+'-'+
                                    str(st.key_for(run,dtype)).split('-')[-1])

corrupted_dids = np.array(corrupted_dids)
np.save('/project2/lgrandi/yuanlq/shared/midway_corrupted/20230821/'+'purge_%s.npy'%(index), corrupted_dids)
