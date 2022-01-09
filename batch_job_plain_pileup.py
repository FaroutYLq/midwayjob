import numpy as np
import time
import os, shlex
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
        jobname = 'photon_pile_up_top{:03}'.format(loop_index)
        z_indices = loop_item
        # Modify here for the script to run
        jobstring = "python /home/yuanlq/software/midwayjob/photon_pile_up_top.py %s"%(z_indices)
        print(jobstring)

        # Modify here for the log name
        utilix.batchq.submit_job(
            jobstring, log='/home/yuanlq/.tmp_job_submission/photon_pile_up_top%s.log'%(z_indices), partition='xenon1t', qos='xenon1t',
            account='pi-lgrandi', jobname=jobname,
            delete_file=True, dry_run=False, mem_per_cpu=10000,
            container='xenonnt-development.simg',
            cpus_per_task=1)

p = Submit()

# Modify here for the runs to process
loop_over = np.array([0,1,2,3,4,5,6,7,8,9])
print('Z indicies to proceed: ', len(loop_over))

p.execute(loop_over=loop_over, max_num_submit=21, nmax=10000)
