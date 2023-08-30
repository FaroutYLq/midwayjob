import admix
import cutax
import pickle
import sys
from tqdm import tqdm
import numpy as np
import os
import matplotlib.pyplot as plt
import gc

OUTPUT_DIR = '/project2/lgrandi/yuanlq/shared/midway_corrupted/20230831/results/'
os.make_dir(OUTPUT_DIR, exist_ok=True)

_, directory, index = sys.argv
print("The directory to process:", directory)

st = cutax.xenonnt_offline()

# Just a concept. It is too slow to be run here.

dtypes_to_check = ["peaklets", "lone_hits", "merged_s2s"]

runs = np.load(directory, allow_pickle=True)

peaks_not_loadbale = []

for run in tqdm(runs):
    run = str(run).zfill(6)
    if st.is_stored(run, 'peaklets') and st.is_stored(run, 'lone_hits') and st.is_stored(run, 'merged_s2s'):
        try:
            peaks = st.get_array(run, 'peaks', keep_columns=('time'))
        except:
            peaks_not_loadbale.append(run)
            continue

np.save(OUTPUT_DIR+"peaks_not_loadable_%s.npy"%(index), np.array(peaks_not_loadbale))

