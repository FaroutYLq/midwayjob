# midwayjob
Job submitter for MIDWAY.



### How to submit jobs on midway

- Prepare your scripts to run, for example

  ```python
  import strax
  import straxen
  from tqdm import tqdm
  #import cutax
  import numpy as np
  import sys
  
  import numpy as np
  from scipy import stats, interpolate, optimize
  
  from cycler import cycler
  import matplotlib
  from collections import OrderedDict
  from multihist import Histdd, Hist1d
  import glob
  import bokeh.plotting as bklt
  
  st = straxen.contexts.xenonnt_online(output_folder='/dali/lgrandi/yuanlq/s1_wf_comparison')
  
  _, runid = sys.argv
  
  st.make(run_id=runid,targets="event_info_double")
  ```

- Prepare a job submitting script, for example

  ```python
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
          _start = 1
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
          jobname = 'event_processing{:03}'.format(loop_index)
          run_id = loop_item
          jobstring = f'python /home/yuanlq/nTslave/s1_pulse_shape/process_events.py {run_id}'
          print(jobstring)
  
          utilix.batchq.submit_job(
              jobstring, log=f'/home/yuanlq/.tmp_job_submission/{run_id}.log', partition='xenon1t', qos='xenon1t',
              account='pi-lgrandi', jobname=jobname,
              delete_file=True, dry_run=False, mem_per_cpu=10000,
              container='xenonnt-development.simg',
              cpus_per_task=1)
  
  p = Submit()
  
  # The runids to process
  loop_over = np.array(['026414', '026411', '026409', '026407', '026405', '026403',
         '026401', '026399', '026168', '026165', '026162', '026159',
         '026156', '026153', '026150', '026147', '026144', '026141',
         '026138', '026135'])
  print(len(loop_over))
  
  p.execute(loop_over=loop_over, max_num_submit=21, nmax=10000)
  ```

- Activate strax environment by `source activate strax `

  - You will need to add several things to your bash_profile first, Like:

    ```shell
    # .bash_profile
    
    # Get the aliases and functions
    if [ -f ~/.bashrc ]; then
            . ~/.bashrc
    fi
    
    # User specific environment and startup programs
    
    PATH=$PATH:$HOME/bin
    CVMFSDIR=/cvmfs/xenon.opensciencegrid.org
    export PATH="${CVMFSDIR}/releases/anaconda/2.4/bin:$PATH"
    export PATH
    
    alias squeueu='squeue -u yuanlq'
    alias cdscratch='cd /scratch/midway2/yuanlq/'
    alias rm='rm -I'
    alias lsl='ls | wc -l'
    ```

- Run your job submitting script
