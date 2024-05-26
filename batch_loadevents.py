import numpy as np
import time
import os, shlex
import pickle
#from subprocess import Popen, PIPE, STDOUT, TimeoutExpired
import utilix
from utilix.batchq import *
print(utilix.__file__)

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
        jobname = 'loading_%s'%(loop_item)
        run_id = loop_item
        # Modify here for the script to run
        jobstring = "python /home/yuanlq/software/midwayjob/load_events.py %s"%(run_id)
        print(jobstring)

        # Modify here for the log name
        utilix.batchq.submit_job(
            jobstring=jobstring, log='/home/yuanlq/.tmp_job_submission/load/load_events%s.log'%(run_id), 
            partition='caslake', qos='caslake',
            account='pi-lgrandi', jobname=jobname,
            mem_per_cpu=5000,
            container='xenonnt-development.simg',
            cpus_per_task=1)

p = Submit()

# Modify here for the runs to process
with open('/project2/lgrandi/xenonnt/reprocessing_runlist/global_v13/runlists_reprocessing_global_v13.pickle', 'rb') as f:
    jingqiang13 = pickle.load(f)
loop_over = jingqiang13['runlists']['sr1_bkg']

print('Runs to process: ', len(loop_over))

p.execute(loop_over=loop_over, max_num_submit=5000, nmax=10000)
