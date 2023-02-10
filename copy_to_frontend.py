import cutax
import straxen
import strax
import sys
import os

_, target = sys.argv
print(os.system('ls /dali/lgrandi/yuanlq'))
straxen.print_versions()

print()

st = cutax.contexts.xenonnt_v8(output_folder='/project2/lgrandi/xenonnt/processed', include_rucio_remote=True)

#st.storage += [strax.DataDirectory('/project2/lgrandi/event_data')]
#assert len(st.storage) == 5, 'don\'t change too many thing at the same time please'
for run in strax.utils.tqdm(
    st.select_runs(run_mode='ambe_linked', available=(target))['number'].values
    ):
    st.copy_to_frontend(str(run).zfill(6), target)
