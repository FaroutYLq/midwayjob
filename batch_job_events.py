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
        jobstring = "python /home/yuanlq/software/midwayjob/process_events.py %s"%(run_id)
        print(jobstring)

        # Modify here for the log name
        utilix.batchq.submit_job(
            jobstring, log='/home/yuanlq/.tmp_job_submission/manual_process/sr0_bkg_unfinished/process_events%s.log'%(run_id), partition='lgrandi', qos='lgrandi',
            account='pi-lgrandi', jobname=jobname,
            delete_file=True, dry_run=False, mem_per_cpu=75000,
            container='xenonnt-2023.10.1.simg',
            cpus_per_task=1)

p = Submit()

# Modify here for the runs to process
#"""SR0 BKG unfinshed
loop_over = np.array(['031513', '031512',
       '031507', '031256', '031243', 
       '031235', '031224', '031218', '031217', '031215',
       '031196', '031195', '031185', 
       '031173', '031161', 
       '031152', '031146', '031133',
       '031087', '031076', '031072', '031031', '030175', '029891',
       '029798', '029743'])
#"""
"""SR0 Rn220 unfinshed batch 0
loop_over = np.array(['024075', '024072', '024068', '024065', '024062'])
"""
"""SR0 Rn220 unfinshed batch 1
loop_over = np.array(['024031', '024028', '024023', '024018', '024015', '024012',
       '023991', '023988', '023985', '023982', '023936', '023927',
       '023925', '023922', '023919', '023892', '023889', '023881',
       '023831', '023828'])
"""
"""What's that bkg?
loop_over=np.array(['031513', '031512', '031507', '031256', '031243', '031235',
       '031224', '031218', '031217', '031215', '031196', '031195',
       '031185', '031173', '031161', '031152', '031146', '031133',
       '031087', '031076', '031072', '031031', '030175', '029891',
       '029798', '029743'])
"""
#loop_over=np.array(['029743','029798','029891','030175','031031','031072','031076', '031087', '031133','031152','031185','031217','031507'])

print('Runs to process: ', len(loop_over))

p.execute(loop_over=loop_over, max_num_submit=100, nmax=10000)
