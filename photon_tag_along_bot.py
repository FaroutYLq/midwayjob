from scipy import special
import numpy as np
import scipy.integrate as integrate
import straxen
from tqdm import tqdm
from scipy.stats import binom
import math
import sys

sys.path.append('/home/yuanlq/xenon/combpile')

import combpile
import s1pattern

_, i = sys.argv # index for z group

print('Begin to process bottom array tag-along fraction for z-cluster %s'%(i))
i = eval(i)

s1_pattern_map = s1pattern.make_map(map_file="/home/yuanlq/software/private_nt_aux_files/sim_files/XENONnT_s1_xyz_patterns_LCE_corrected_qes_MCva43fa9b_wires.pkl", fmt=None, method='WeightedNearestNeighbors')

afts = []
for k in range(10):
    indices = (15,15,12+8*k) # just convered FV: -134.1021675cm - -17.719913100000014cm
    aft = s1_pattern_map.data['map'][indices[0],indices[1],indices[2],:253].sum()/s1_pattern_map.data['map'][indices[0],indices[1],indices[2],:].sum()
    afts.append(aft)

n_resol = 100
ns = np.linspace(2, 1000, n_resol, dtype=int) 
tag_frac_bot = np.zeros((n_resol))
z_range_fv = np.linspace(-134.238, -13.6132, 11)

occupancies, degeneracies = s1pattern.get_pattern(z_range_fv[i:i+2], False)
for j in range(n_resol):
    print('Started to process z group index ',ns[j])
    tag_frac_bot[j] = combpile.P_tag_n(n=ns[j], top=False, occupancies=occupancies, degeneracies=degeneracies, aft=afts[i])

print('Begin to save top array tag-along fraction for z-cluster %s'%(i))

np.save('/home/yuanlq/xenon/combpile/maps/phd_tag_along_frac_bot_z%s.npy'%(i), tag_frac_bot)

print('Finished saving. Exiting.')