import numpy as np
import time
import os, shlex
#from subprocess import Popen, PIPE, STDOUT, TimeoutExpired
import utilix
from utilix.batchq import *
import pickle
print(utilix.__file__)

def chunk_list(lst, chunk_size=50):
    """Divide a list into chunks of a specified size.
    
    Args:
        lst (list): The list to be chunked.
        chunk_size (int): The maximum number of items in each chunk.
        
    Returns:
        list of lists: A list containing the chunked lists.
    """
    # List comprehension that generates chunks from the list
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

class Submit(object):
    '''
        Take maximum number of nodes to use at once
        Submit each group to a node and excute
    '''
    def name(self):
        return self.__class__.__name__

    def execute(self, *args, **kwargs):
        eval('self.{name}(*args, **kwargs)'.format(name = self.name().lower()))

    def submit(self, loop_over=[], max_num_submit=10, nmax=3):
        _start = 0
        self.max_num_submit = max_num_submit
        self.loop_over = loop_over
        self.p = True

        index = _start
        while (index < len(self.loop_over) and index < nmax):
            if (self.working_job() < self.max_num_submit):
                self._submit_single(loop_index=index,
                                    loop_item=self.loop_over[index])

                time.sleep(1.0)
                index += 1

    # check my jobs
    def working_job(self):
        cmd='squeue --user={user} | wc -l'.format(user = 'yuanlq')
        jobNum=int(os.popen(cmd).read())
        return  jobNum -1

    def _submit_single(self, loop_index, loop_item):
        batch_i = loop_index
        jobname = 'loading_%s'%(batch_i)
        # Modify here for the script to run
        jobstring = "python /home/yuanlq/loadtest/load_peaks.py '%s'"%(list(loop_item))
        print(jobstring)

        # Modify here for the log name
        utilix.batchq.submit_job(
            jobstring=jobstring, log='/dali/lgrandi/yuanlq/logs/loadtest/sr1_rn222/load_peaks_batch%s.log'%(batch_i), 
            partition='dali', qos='dali',
            account='pi-lgrandi', jobname=jobname,
            mem_per_cpu=40000,
            container='xenonnt-development.simg',
            cpus_per_task=1)

p = Submit()

# Modify here for the runs to process
with open('/project2/lgrandi/xenonnt/reprocessing_runlist/global_v13/runlists_reprocessing_global_v13.pickle', 'rb') as f:
    all_runs = pickle.load(f)
interested_runlist = all_runs['runlists']['sr1_rn222']
loop_over = chunk_list(interested_runlist, chunk_size=40)
print(loop_over)

print('Batches to process: ', len(loop_over))

p.execute(loop_over=loop_over, max_num_submit=5000, nmax=10000)
