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
        run_id = loop_item
        jobname = 'process_events{}'.format(run_id)
        # Modify here for the script to run
        jobstring = "python /home/yuanlq/software/midwayjob/process_events.py %s"%(run_id)
        print(jobstring)

        # Modify here for the log name
        utilix.batchq.submit_job(
            jobstring=jobstring, log='/dali/lgrandi/yuanlq/process_events_20240601/process_events%s.log'%(run_id), 
            partition='dali', qos='dali',
            account='pi-lgrandi', jobname=jobname,
            dry_run=False, mem_per_cpu=40000, #exclude_nodes='dali001,dali003,dali005',
            container='xenonnt-development.simg',
            cpus_per_task=1)

p = Submit()

# filtered out finished ones
loop_over =['052155', '052378', '049374', '052889', '049376', '052620', '052289', '049402', '052893', '052571', '049375', '052375', '052260', '053504', '053445', '052233', '052157', '052557', '052895', '052351', '049395', '052547', '053224', '050099', '049319', '049337', '052326', '052306', '052154', '052361', '053446', '052312', '052604', '052309', '052602', '052296', '052606', '052894', '049386', '052598', '053221', '050722', '052383', '052235', '052797', '053505', '052380', '051816', '052364', '049382', '050080', '053503', '052892', '051822', '052305', '052960', '052282', '052286', '052905', '052565', '052605', '053222', '052603', '044687', '052611', '053216', '051814', '052969', '049369', '049368', '052712', '052711', '052713', '053245', '052292', '052562', '049389', '053422', '050774', '052096', '052720']
print('Runs to process: ', len(loop_over))

p.execute(loop_over=loop_over, max_num_submit=5000, nmax=10000)
