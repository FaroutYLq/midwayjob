import admix
import cutax
import pickle
import sys
from tqdm import tqdm
import numpy as np
import os
import matplotlib.pyplot as plt

OUTPUT_DIR = '/project2/lgrandi/yuanlq/shared/midway_corrupted/20230729/results/'

_, directory, index = sys.argv
print("The directory to process:", directory)

def find_missing_chunk(files, should_have_n_chunks):
    chunk_numbers = []
    for file in files:
        chunk_str = file.split('-')[-1]
        chunk_id = int(chunk_str)
        chunk_numbers.append(chunk_id)
    chunk_numbers = np.array(chunk_numbers)
    should_have_numbers = np.arange(should_have_n_chunks)
    missing_ids = np.setdiff1d(should_have_numbers, chunk_numbers)
    return missing_ids

def file_exists_in_directory(filename='peaklets-ui5hguaz2k-000302', directory='/project/lgrandi/rucio/xnt_049571'):
    for root, dirs, files in os.walk(directory):
        if filename in files:
            return True
    return False

st = cutax.xenonnt_offline()

# Just a concept. It is too slow to be run here.

dtypes_to_check = ["peaklets", "lone_hits", "merged_s2s"]
hashes = {"peaklets": "ui5hguaz2k",
          "lone_hits": "ui5hguaz2k",
          "merged_s2s": "celut6blxq"}


runs = np.load(directory, allow_pickle=True)

incomplete_dids = []
incomplete_dids_loadable = {}
missing_file_dids = {}
missing_files_on_disk = {}

for run in tqdm(runs):
    run = str(run).zfill(6)
    for dtype in dtypes_to_check:
        did = 'xnt_%s:%s-%s'%(run, dtype, hashes[dtype])
        if st.is_stored(run, dtype):
            files = admix.rucio.list_files(did)
            should_have_n_chunks = st.get_metadata(run, dtype)["chunks"][-1]["chunk_i"]+1
            if len(files)-1 != should_have_n_chunks:
                incomplete_dids.append(did)
                try:
                    temp = st.get_array(run, dtype, keep_columns=('time'))
                    incomplete_dids_loadable[did] = True
                except:
                    incomplete_dids_loadable[did] = False
                missing_ids = find_missing_chunk(files[:-1], should_have_n_chunks)
                on_disk = []
                missing_file_dids[did] = np.array([did+'-'+str(cid).zfill(6) for cid in missing_ids])
                for f_did in missing_file_dids[did]:
                    on_disk.append(file_exists_in_directory(filename=f_did.split(':')[-1], 
                                                            directory='/project/lgrandi/rucio/'+f_did.split(':')[0]))
                missing_files_on_disk[did] = np.array(on_disk)


# create a binary pickle file 
f1 = open(OUTPUT_DIR+"incomplete_dids_loadable_chunk_%s.pkl"%(index),"wb")
# write the python object (dict) to pickle file
pickle.dump(incomplete_dids_loadable,f1)
# close file
f1.close()

# create a binary pickle file 
f2 = open(OUTPUT_DIR+"missing_file_dids_chunk_%s.pkl"%(index),"wb")
# write the python object (dict) to pickle file
pickle.dump(missing_file_dids,f2)
# close file
f2.close()

# create a binary pickle file 
f3 = open(OUTPUT_DIR+"missing_files_on_disk_chunk_%s.pkl"%(index),"wb")
# write the python object (dict) to pickle file
pickle.dump(missing_files_on_disk,f3)
# close file
f3.close()
