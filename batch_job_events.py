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
            jobstring=jobstring, log='/dali/lgrandi/yuanlq/dacheng236/process_events%s.log'%(run_id), 
            partition='dali', qos='dali',
            account='pi-lgrandi', jobname=jobname,
            dry_run=False, mem_per_cpu=45000, #exclude_nodes='dali001,dali003,dali005',
            container='xenonnt-2023.10.1.simg',
            cpus_per_task=1)

p = Submit()

loop_over = ['052738', '047621', '052745', '045067', '052747', '052750',
       '047631', '048660', '050714', '047643', '050718', '047647',
       '047648', '050719', '050720', '050721', '047653', '044582',
       '044071', '052776', '052777', '052778', '052779', '052780',
       '053293', '047661', '043055', '044584', '049201', '052782',
       '043571', '044589', '052785', '052786', '052787', '052788',
       '052789', '044598', '052790', '052284', '043581', '052285',
       '052791', '052288', '052792', '050747', '052291', '043588',
       '043589', '050751', '050758', '050760', '053321', '052299',
       '044107', '050765', '050769', '051794', '053331', '048211',
       '051797', '044118', '051799', '051800', '051801', '051802',
       '052313', '051804', '051805', '051807', '051808', '051810',
       '051811', '050786', '050790', '050791', '051822', '043119',
       '046719', '046720', '046721', '052868', '050823', '052363',
       '052365', '046734', '050831', '044687', '050833', '046735',
       '046738', '051860', '051861', '050836', '050837', '049304',
       '051864', '049306', '051865', '044184', '052377', '052378',
       '044189', '050839', '050840', '052897', '052898', '049316',
       '051877', '051878', '051879', '049320', '052902', '052903',
       '052908', '046765', '052910', '046768', '047282', '052915',
       '043700', '052920', '052922', '049864', '049871', '049872',
       '052949', '045276', '048351', '044256', '052959', '052969',
       '052970', '048365', '048366', '052973', '045296', '052974',
       '047348', '049403', '048382', '053503', '053504', '053505',
       '048385', '048387', '050945', '047365', '048389', '046739',
       '043822', '048430', '046900', '048439', '043839', '052544',
       '052545', '053055', '053060', '048456', '052553', '052554',
       '052558', '048463', '053072', '053075', '046932', '053078',
       '052567', '052568', '043865', '053082', '048476', '050748',
       '046943', '046944', '048484', '048486', '048487', '050025',
       '050026', '050027', '050028', '047473', '047474', '050036',
       '047477', '052084', '050039', '050040', '052086', '053116',
       '050046', '050047', '053120', '052100', '052613', '053126',
       '047495', '052104', '047497', '052617', '053133', '047506',
       '052794', '050070', '048535', '050074', '047515', '052796',
       '048546', '046499', '050085', '050086', '050092', '052149',
       '048567', '047548', '047551', '052159', '047556', '047565',
       '052176', '047573', '050772', '050655', '053215', '050658',
       '050662', '050663', '052715', '052716', '052717', '052719',
       '052721', '052724', '052726', '052728', '052729', '044026']
print('Runs to process: ', len(loop_over))

p.execute(loop_over=loop_over, max_num_submit=5000, nmax=10000)
