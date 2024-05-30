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
            jobstring=jobstring, log='/dali/lgrandi/yuanlq/process_events/process_events%s.log'%(run_id), 
            partition='dali', qos='dali',
            account='pi-lgrandi', jobname=jobname,
            dry_run=False, mem_per_cpu=15000, #exclude_nodes='dali001,dali003,dali005',
            container='xenonnt-development.simg',
            cpus_per_task=1)

p = Submit()

loop_over = ['043055', '047548', '050070', '051864', '052780', '043119',
       '047551', '050074', '051865', '052782', '043571', '047556',
       '050085', '051877', '052785', '043581', '047565', '050086',
       '051878', '052786', '043588', '047573', '050092', '051879',
       '052787', '043589', '047631', '050655', '052084', '052788',
       '043700', '047643', '050658', '052086', '052789', '043822',
       '047647', '050662', '052100', '052790', '043839', '047648',
       '050663', '052104', '052791', '043865', '047653', '050714',
       '052149', '052792', '044026', '047661', '050718', '052159',
       '052794', '044071', '048211', '050719', '052176', '052796',
       '044107', '048351', '050720', '052284', '052868', '044118',
       '048365', '050721', '052285', '052897', '044184', '048366',
       '050747', '052288', '052898', '044189', '048382', '050748',
       '052291', '052902', '044582', '048385', '050751', '052299',
       '052903', '044584', '048387', '050758', '052313', '052908',
       '044589', '048389', '050760', '052363', '052910', '044598',
       '048430', '050765', '052365', '052915', '044687', '048439',
       '050769', '052377', '052920', '045067', '048456', '050772',
       '052378', '052922', '045276', '048463', '050786', '052544',
       '052949', '045296', '048476', '050790', '052545', '052959',
       '046499', '048484', '050791', '052553', '052969', '046719',
       '048486', '050823', '052554', '052970', '046720', '048487',
       '050831', '052558', '052973', '046721', '048535', '050833',
       '052567', '052974', '046734', '048567', '050836', '052568',
       '053055', '046735', '048660', '050837', '052613', '053060',
       '046738', '049201', '050839', '052617', '053072', '046739',
       '049304', '050840', '052715', '053075', '046765', '049306',
       '050945', '052716', '053078', '046768', '049316', '051794',
       '052717', '053082', '046900', '049320', '051797', '052719',
       '053116', '046932', '049403', '051799', '052721', '053120',
       '046943', '049864', '051800', '052724', '053126', '046944',
       '049871', '051801', '052726', '053133', '047282', '049872',
       '051802', '052728', '053215', '047348', '050025', '051804',
       '052729', '053293', '047365', '050026', '051805', '052738',
       '053321', '047473', '050027', '051807', '052745', '053331',
       '047474', '050028', '051808', '052747', '053503', '047477',
       '050036', '051810', '052750', '053504', '047495', '050039',
       '051811', '052776', '053505', '047497', '050040', '051822',
       '052777', '047506', '050046', '051860', '052778', '047515',
       '050047', '051861', '052779']
print('Runs to process: ', len(loop_over))

p.execute(loop_over=loop_over, max_num_submit=5000, nmax=10000)
