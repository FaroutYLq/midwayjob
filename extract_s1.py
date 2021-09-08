import strax
import straxen
from tqdm import tqdm
#import cutax
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from multiprocessing import Pool
import nestpy

import numpy as np
from scipy import stats, interpolate, optimize

from cycler import cycler
import matplotlib
from collections import OrderedDict
from multihist import Histdd, Hist1d
import glob
import bokeh.plotting as bklt

st = straxen.contexts.xenonnt_online(output_folder='/dali/lgrandi/yuanlq/s1_wf_comparison')

run_numbers = np.array(['026414', '026411', '026409', '026407', '026405', '026403'])

run_numbers
events=st.get_array(run_id=run_numbers,targets="event_info_double")

# bug fixing (no harm to new versions)
events['ds_s1_dt'] = events['s1_b_center_time']-events['s1_a_center_time']
events['ds_s2_dt'] = events['s2_b_center_time']-events['s2_a_center_time']

original_counts = len(events)

# S1 AFT cut
def mask_S1_AFT(df):
 
    def line(x,a,b):
        return a*x+b
 
    mask = df['s1_a_area_fraction_top'] < line(df['drift_time'],-2.3e-7,0.70)
    mask &= df['s1_a_area_fraction_top'] > line(df['drift_time'],-2e-7,0.40)
    return mask

events=events[mask_S1_AFT(events)]
print('S1 AFT cut acceptance: ', len(events)/original_counts)
#===========================================================================================================================

# S2 width cut
def mask_S2_Width(df):
 
    def diffusion_model(t,w_SE, w_t0, t_0):
        return np.sqrt(w_SE**2 + ((w_t0 - w_SE)**2 /t_0) * t)
 
    w_SE= 599.70428e-3
    w_t0= 400.29572e-3
    t_0= 1.0029191e-3
 
    mask = df['s2_a_range_50p_area']/diffusion_model(df['drift_time'], w_SE, w_t0, t_0) > -30/(df['drift_time']*1e-3+10)+0.8
    mask &= df['s2_a_range_50p_area']/diffusion_model(df['drift_time'], w_SE, w_t0, t_0) < 30/(df['drift_time']*1e-3+10)+1.2
    mask &= df['drift_time']*1e-3 < 2400
    return mask

events=events[mask_S2_Width(events)]
print('S1 AFT cut + S2 AFT cut acceptance: ', len(events)/original_counts)
#===========================================================================================================================

# Double S1 cut
def mask_KrDoubleS1(df):
    mask = (df['s1_a_n_channels'] >= 80) & (df['s1_a_n_channels'] < 225)
    mask &= (df['s1_b_n_channels'] >= 25) & (df['s1_b_n_channels'] < 125)
    mask &= (df['s1_b_distinct_channels'] >= 0) & (df['s1_b_distinct_channels'] < 60)
    mask &= (df['ds_s1_dt'] >= 750) & (df['ds_s1_dt'] < 2000)
    return mask

events=events[mask_KrDoubleS1(events)]
print('Double S1 cut + S1 AFT cut + S2 AFT cut acceptance: ', len(events)/original_counts)
#===========================================================================================================================

# These two functions might be too awkward in computational efficiency:
# We spent too much time on loading data again and again

def find_peak(s1_time, s1_run_id, context):
    """
    Find the a specfic S1 in peaklets.
    """
    peaklets = context.get_array(s1_run_id, 'peaklets',
                                time_range=(s1_time-10000,s1_time+10000))
    wanted_s1 = peaklets[peaklets['time']==s1_time]
    if len(wanted_s1) == 0:
        print('Peak not found')
    else:
        return wanted_s1
    
from tqdm import tqdm
def extract_peaks(event_info, context, ab='a'):
    """
    Very slow, but requires very little memory.
    """
    if ab == 'a':
        time_string = 's1_a_time'
    elif ab == 'b':
        time_string = 's1_b_time'
    else:
        return 'Wrong input for ab, please try either "a" or "b"'
    for k,event in tqdm(enumerate(event_info)):
        if k==0:
            peaklet = find_peak(event[time_string], event['run_id'], context)
            peaklets = peaklet
        else:
            peaklet = find_peak(event[time_string], event['run_id'], context)
            peaklets = np.append(peaklets, peaklet)
    return peaklets

def is_new_run(events):
    diff_event_id = np.diff(events['event_number'])
    new_run_index = np.where(diff_event_id < 0)[0] + 1
    
    # don't forget the very first one in event
    new_run_index = np.concatenate((np.array([0]),new_run_index))
    
    return new_run_index


def quick_peak_extraction(events, run_numbers, s1_type='a'):
    new_run_index = is_new_run(events)

    if s1_type=='a':
        s1_string = 's1_a_time'
        print('Loading peaklets for S1As')
    elif s1_type=='b':
        s1_string = 's1_b_time'
        print('Loading peaklets for S1Bs')
    else:
        s1_string = 'Unknown S1 Type'
        print('Wrong input for s1_type. Try "a" or "b".')
        
        
    for run_i,new_run_ind in enumerate(new_run_index):
        print('Starting run %s'%(run_numbers[run_i]))
        run_id = run_numbers[run_i]
        peaklets = st.get_array(run_id, 'peaklets')
        if run_i==0: # trivial initialization, will throw it in the end
            print('Loaded first peaklets dataset')
            extracted_peaks = peaklets[peaklets['time']==peaklets[0]['time']]
        
        if run_i != len(run_numbers) - 1: # Last run 
            event_info_double = events[new_run_ind:new_run_index[run_i+1]]
        else:
            event_info_double = events[new_run_ind:]
        
        for j,e in enumerate(event_info_double):
            extracted_peak = peaklets[peaklets['time']==e['s1_a_time']]
            if len(extracted_peak) != 0:
                #print(type(extracted_peaks))
                #print(extracted_peak.shape)
                extracted_peaks = np.concatenate((extracted_peaks,extracted_peak))
    
    return extracted_peaks[1:]

import gc
gc.collect()
peaklets_krs1a = quick_peak_extraction(events = events, run_numbers = run_numbers, s1_type='a')
np.save('/dali/lgrandi/yuanlq/s1_wf_comparison/peaklets_krs1a_updated',peaklets_krs1a)

print('Saved S1a')