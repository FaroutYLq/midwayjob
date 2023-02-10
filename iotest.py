import strax
import straxen
import numpy as np
import sys
import cutax
import datetime

print("Finished importing, now start to load context.")
# Modify below for the strax.storage path

_, runid = sys.argv
print("Loaded the context successfully, and the run id to process:", runid)

nfs = []
for i in range(100):
    st = cutax.contexts.xenonnt_online()
    st.storage = [strax.DataDirectory('/project2/lgrandi/xenonnt/processed')]
    start = datetime.datetime.now()
    peaklets = st.get_array('049914', 'peaklets', seconds_range=(0,200))
    end = datetime.datetime.now()
    dt = end - start
    nfs.append(dt.total_seconds())
nfs = np.array(nfs)
np.save('/project2/lgrandi/yuanlq/test/iotest%s.npy'%(runid), nfs)

print('Finished')
