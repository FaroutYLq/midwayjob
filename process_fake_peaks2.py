import strax
import straxen
import numpy as np
import sys

import numpy as np

print("Finished importing, now start to load context.")
# Modify below for the strax.storage path
print(straxen.print_versions())
sys.path.append('/home/yuanlq/data-driven-S1-efficiency')
from data_driven_sampling import Fake_Peaklets

st_fake_context = straxen.contexts.xenonnt_online(output_folder='/dali/lgrandi/eangelino/s1_efficiency_data/')

st_fake_context.storage.append(strax.DataDirectory('/dali/lgrandi/eangelino/s1_efficiency_data',
                                      readonly=True)
                 )

st_fake_context.register(Fake_Peaklets)

_, runid = sys.argv
print("Loaded the context successfully, and the run id to process:", runid)

print("Start to make data for Ar parents without replacement")

st_fake_context.make(runid,targets="peaklets",config=dict(parent_s1_type='KrS1B',
                    replace_hit=False, n_repeat=30, upper_rhits_parent_fraction=0.5))

print('Done!')
