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
        jobname = 'process_events{:03}'.format(loop_index)
        run_id = loop_item
        # Modify here for the script to run
        jobstring = "python /home/yuanlq/software/midwayjob/process_peaklets.py %s"%(run_id)
        print(jobstring)

        # Modify here for the log name
        utilix.batchq.submit_job(
            jobstring, log='/home/yuanlq/.tmp_job_submission/process_peaklets%s.log'%(run_id), partition='xenon1t', qos='xenon1t',
            account='pi-lgrandi', jobname=jobname,
            delete_file=True, dry_run=False, mem_per_cpu=16000,
            container='xenonnt-development.simg',
            cpus_per_task=1)

p = Submit()

# Modify here for the runs to process

#loop_over = np.array(['047625','048153', '052150','051383', '051746', '052054','051389', '050669', '050847'])
loop_over = np.array(['034310', '034307', '034304', '034301', '034298', '034295', '034292', '034289', '034286', '020412', '020396', '020402', '020392', '020394', '020387', '020384', '020382', '020380', '020381', '034695', '034692', '034689', '034686', '034683', '034680', '034677', '034674', '034671', '034668', '026076', '026075', '026074', '026073', '026072', '026071', '026070', '026069', '026068', '026067'])

print('Runs to process: ', len(loop_over))

p.execute(loop_over=loop_over, max_num_submit=21, nmax=10000)
