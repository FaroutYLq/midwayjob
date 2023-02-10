import strax
import straxen
import numpy as np
import sys

import numpy as np

print("Finished importing, now start to load context.")
# Modify below for the strax.storage path
print(straxen.print_versions())
sys.path.append('/home/yuanlq/xenon/data-driven-S1-efficiency')
from data_driven_sampling import Fake_Peaklets
from update_sampled_hits_info import TruePeaksProcessing

st_fake_context = straxen.contexts.xenonnt_online(output_folder='/dali/lgrandi/eangelino/s1_efficiency_data/')

st_fake_context.storage.append(strax.DataDirectory('/dali/lgrandi/eangelino/s1_efficiency_data',
                                      readonly=True)
                 )

st_fake_context.register(Fake_Peaklets)
st_fake_context.register(TruePeaksProcessing)

_, runid = sys.argv
print("Loaded the context successfully, and the run id to process:", runid)

st_fake_context.make(runid,targets="true_peaks",config=dict(parent_s1_type='KrS1B', s1_min_coincidence=2,
                    s2_merge_max_duration=3000,
                    replace_hit=True, n_repeat=30, upper_rhits_parent_fraction=0.9))

"""
st_fake_context.make(runid,targets="true_peaks",config=dict(parent_s1_type='Ar', s1_min_coincidence=3,
                    s2_merge_max_duration=3000,
                    replace_hit=True, n_repeat=10, upper_rhits_parent_fraction=0.9))
"""
print('Done!')
