import numpy as np
import jax.numpy as jnp
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import multihist as mh
import sys

import GOFevaluation

import appletree as apt
from appletree.utils import get_file_path

type_dict = {
    "/home/yuanlq/analysis/salt_rn220_sr0.json": "salt",
    "/home/yuanlq/analysis/simu_rn220_sr0.json": "simu"
}
_, config_file_path = sys.argv
typed = type_dict[config_file_path]
apt.set_gpu_memory_usage(0.2)

# Load configuration file
config = get_file_path(config_file_path)
# Initialize context
tree = apt.Context(config)

# To see all the likelihoods
tree.print_context_summary(short=True)

# Actual fitting
result = tree.fitting(nwalkers=200, iteration=500)

# Template HISTDD
cs1, cs2, eff = tree.get_template("rn220_llh", "rn220_er")
np.save('/project/lgrandi/yuanlq/salted/%s_template_cs1.npy'%(typed), cs1)
np.save('/project/lgrandi/yuanlq/salted/%s_template_cs2.npy'%(typed), cs2)
np.save('/project/lgrandi/yuanlq/salted/%s_template_eff.npy'%(typed), eff)
