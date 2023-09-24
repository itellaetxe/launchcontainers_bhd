import os
import subprocess as sp
import numpy as np
import logging
import shlex
# Dask imports
from dask import delayed as delayed_dask
from dask.distributed import progress

# modules in lc
import dask_schedule_queue as dsq
import prepare_input as prepare
import utils as do


logger=logging.getLogger("GENERAL")



# %% launchcontainers
def cluster_cmdrun_dwi(host,path_to_sub_derivatives,path_to_config_json,sif_path,logfilename,run_it):
    logger.info("\n"+ f'start to generate the DWI PIPELINE command')
    if "BCBL" in host:
        cmdcmd=f"singularity run -e --no-home "\
            f"--bind /bcbl:/bcbl "\
            f"--bind /tmp:/tmp "\
            f"--bind /export:/export "\
            f"--bind {path_to_sub_derivatives}/input:/flywheel/v0/input:ro "\
            f"--bind {path_to_sub_derivatives}/output:/flywheel/v0/output "\
            f"--bind {path_to_config_json}:/flywheel/v0/config.json "\
            f"{sif_path} 2>> {logfilename}.e 1>> {logfilename}.o "
    elif "DIPC" in host:
        cmdcmd=f"singularity run -e --no-home "\
            f"--bind /scratch:/scratch "\
            f"--bind {path_to_sub_derivatives}/input:/flywheel/v0/input:ro "\
            f"--bind {path_to_sub_derivatives}/output:/flywheel/v0/output "\
            f"--bind {path_to_config_json}:/flywheel/v0/config.json "\
            f"{sif_path} 2>> {logfilename}.e 1>> {logfilename}.o "
    if run_it:
        sp.run(cmdcmd,shell=True)
    else:
        pass    
    if cmdcmd is None:
        logger.error("\n"+ f'the DWI PIPELINE command is not assigned, please check your config.yaml[general][host] session\n')
        raise ValueError('cmd is not defiend, aborting')
    return cmdcmd
# %% launchcontainers
def cluster_cmdrun_fmriprep(sub,host,homedir,basedir,fs_lisense,Dir_analysis,sif_path, run_it):
    logger.info("\n"+ f'start to generate the FMRIPREP command')
    # so this code, the cmd code for bcbl, dipc, is the same, all this function is to have a runned sp.run, so that the dask can work 
    cmd= f'module unload singularity/3.5.2; '\
            f'module load singularity/3.5.2; '\
            f'mkdir -p {homedir}; '\
            f'unset PYTHONPATH; ' \
            f'singularity run '\
            f'-H {homedir} '\
            f'-B {basedir}:/base -B {fs_lisense}:/license '\
            f'--cleanenv {sif_path} '\
            f'-w /base/derivatives/fmriprep/analysis-sessplit '\
            f'/base/BIDS {Dir_analysis} participant '\
                f'--participant-label sub-{sub} '\
            f'--skip-bids-validation '\
            f'--output-spaces func fsnative fsaverage T1w MNI152NLin2009cAsym '\
            f'--dummy-scans 0 '\
            f'--use-syn-sdc '\
            f'--fs-license-file /license/license.txt '\
            f'--nthreads {nthreads} '\
            f'--omp-nthreads {nthreads} '\
            f'--stop-on-first-crash '\
            f'--mem_mb {(mem*1000)-5000} '\

    if run_it:
        sp.run(cmd,shell=True)
    else:
        pass    
    if cmdcmd is None:
        logger.error("\n"+ f'the fmriprep is not assigned, please check your config.yaml[general][host] session\n')
        raise ValueError('cmd is not defiend, aborting')
    return cmd
def cluster_cmdrun_dwi(host,path_to_sub_derivatives,path_to_config_json,sif_path,logfilename,run_it):
    logger.info("\n"+ f'start to generate the DWI PIPELINE command')
    if "BCBL" in host:
        cmdcmd=f"singularity run -e --no-home "\
            f"--bind /bcbl:/bcbl "\
            f"--bind /tmp:/tmp "\
            f"--bind /export:/export "\
            f"--bind {path_to_sub_derivatives}/input:/flywheel/v0/input:ro "\
            f"--bind {path_to_sub_derivatives}/output:/flywheel/v0/output "\
            f"--bind {path_to_config_json}:/flywheel/v0/config.json "\
            f"{sif_path} 2>> {logfilename}.e 1>> {logfilename}.o "
    elif "DIPC" in host:
        cmdcmd=f"singularity run -e --no-home "\
            f"--bind /scratch:/scratch "\
            f"--bind {path_to_sub_derivatives}/input:/flywheel/v0/input:ro "\
            f"--bind {path_to_sub_derivatives}/output:/flywheel/v0/output "\
            f"--bind {path_to_config_json}:/flywheel/v0/config.json "\
            f"{sif_path} 2>> {logfilename}.e 1>> {logfilename}.o "
    if run_it:
        sp.run(cmdcmd,shell=True)
    else:
        pass    
    if cmdcmd is None:
        logger.error("\n"+ f'the DWI PIPELINE command is not assigned, please check your config.yaml[general][host] session\n')
        raise ValueError('cmd is not defiend, aborting')
    return cmdcmd
#%% the launchcontainer
def launch_dwi(Dir_analysis, lc_config, sub_ses_list, config_under_analysis, run_it):
    """
    This function launches containers generically in different Docker/Singularity HPCs
    This function is going to assume that all files are where they need to be.

    Parameters
    ----------
    sub_ses_list: Pandas data frame
        Data frame with subject information and if run or not run
    lc_config : dict
        Dictionary with all the values in the configuracion yaml file
    run_it: boolean
        used to control if we run the launchcontainer, or not
    lc_config_path: Str
        the path to lc_config file, it is the input from parser
  
   
   """
    logger.info("\n"+
                "#####################################################\n")

    host = lc_config["general"]["host"]
    jobqueue_config= lc_config["host_options"][host]
    

    basedir = lc_config["general"]["basedir"]
    container = lc_config["general"]["container"] 
    version = lc_config["container_specific"][container]["version"]
    containerdir = lc_config["general"]["containerdir"] 
    sif_path = os.path.join(containerdir, f"{container}_{version}.sif")
    force = lc_config["general"]["force"]

    # Count how many jobs we need to launch from  sub_ses_list
    n_jobs = np.sum(sub_ses_list.RUN == "True")

    client, cluster = dsq.dask_scheduler(jobqueue_config,n_jobs)
    logger.info("---this is the cluster and client\n"
                +f"{client} \n cluster: {cluster} \n")
    
    hosts = []
    paths_to_subs_derivatives = []
    paths_to_configs_json = []
    sif_paths = []
    logfilenames = []
    run_its=[]
    #future_for_print=[]
    for row in sub_ses_list.itertuples(index=True, name='Pandas'):
        sub  = row.sub
        ses  = row.ses
        RUN  = row.RUN
        dwi  = row.dwi
        func = row.func
        if RUN=="True" and dwi=="True":
            tmpdir = os.path.join(
                Dir_analysis,
                "sub-" + sub,
                "ses-" + ses,
                "output", "tmp"
            )
            logdir = os.path.join(
                Dir_analysis,
                "sub-" + sub,
                "ses-" + ses,
                "output", "log"
            )
            backup_configs = os.path.join(
                Dir_analysis,
                "sub-" + sub,
                "ses-" + ses,
                "output", "configs"
            )

            path_to_sub_derivatives=os.path.join(Dir_analysis,
                                                 f"sub-{sub}",
                                                 f"ses-{ses}")

            path_to_config_json = config_under_analysis["new_container_specific_config_path"]
            path_to_config_yaml = config_under_analysis["new_lc_config_path"]
            path_to_subSesList  = config_under_analysis["new_sub_ses_list_path"]


            logfilename=f"{logdir}/t-{container}-sub-{sub}_ses-{ses}"

            if not os.path.isdir(tmpdir):
                os.mkdir(tmpdir)
            if not os.path.isdir(logdir):
                os.mkdir(logdir)
            if not os.path.isdir(backup_configs):
                os.mkdir(backup_configs)
            
            backup_config_json = os.path.join(backup_configs, "config.json")
            backup_config_yaml = os.path.join(backup_configs, "config_lc.yaml")
            backup_subSesList = os.path.join(backup_configs, "subSesList.txt")
            
            hosts.append(host)
            paths_to_subs_derivatives.append(path_to_sub_derivatives)
            paths_to_configs_json.append(path_to_config_json)
            sif_paths.append(sif_path)
            logfilenames.append(logfilename)
            run_its.append(run_it)
            
            if not run_it:
                if host in ['BCBL', 'DIPC']
                    # this cmd is only for print the command 
                    command= cluster_cmdrun_dwi(host,path_to_sub_derivatives,path_to_config_json,sif_path,logfilename,run_it)
                     
                if host =='local'
        
                    command=f"singularity run -e --no-home "\
                        f"--bind {path_to_sub_derivatives}/input:/flywheel/v0/input:ro "\
                        f"--bind {path_to_sub_derivatives}/output:/flywheel/v0/output "\
                        f"--bind {path_to_config_json}:/flywheel/v0/config.json "\
                        f"{sif_path} 2>> {logfilename}.e 1>> {logfilename}.o "
                
                logger.critical("\n"
                                    +f"--------run_lc is false, if True, we would launch this command: \n"
                                    +f"\n this command will be run on the {host}\n"
                                    +f"{command}\n\n"
                                    +"Please check if the job_script is properlly defined and then starting run_lc \n")
    if run_it:
        if host in ['BCBL', 'DIPC']
            futures = client.map(cmdrun_dwi,hosts,paths_to_subs_derivatives,paths_to_configs_json,sif_paths,logfilenames,run_its)
            progress(futures)
            results = client.gather(futures)
            logger.ino(results)
            logger.ino('###########')
        
            client.close()
            cluster.close()
        if host == 'local':
            sp.run(command, shell = True)
        
        do.copy_file(path_to_config_json,backup_config_json,force)
        do.copy_file(path_to_config_yaml, backup_config_yaml, force)
        do.copy_file(path_to_subSesList, backup_subSesList, force)
        logger.info("\n"
                        +f"copied all the config files to {backup_configs} folder")
        logger.critical("\n"
                         + "lanchcontainer finished, all the jobs are done")
    return
#%% 
def launch_fmriprep(Dir_analysis,lc_config,sub_ses_list, run_lc):    
    # gathering the input for cmd
    basedir=lc_config['general']['basedir']
    container= lc_config['general']['container']
    nthreads=lc_config[container]['nthreads']
    mem=lc_config[container]['mem'] 
    fs_lisense=lc_config[container]['fs_lisense']
    sif_path=  lc_config['general']['sif_path']  

    # need some for loop
    for row in sub_ses_list.itertuples(index=True, name='Pandas'):
        sub  = row.sub
        ses  = row.ses
        RUN  = row.RUN
        func = row.func 
        if RUN=="True" and func=="True":
            homedir= os.path.join(
                basedir,
                'singularity_home',
                f'sub-{sub}'
            )

            cmd= f'module unload singularity/3.5.2; '\
                f'module load singularity/3.5.2; '\
                f'mkdir -p {homedir}; '\
                f'unset PYTHONPATH; ' \
                f'singularity run '\
                f'-H {homedir} '\
                f'-B {basedir}:/base -B {fs_lisense}:/license '\
                f'--cleanenv {sif_path} '\
                f'-w /base/derivatives/fmriprep/analysis-sessplit '\
                f'/base/BIDS {Dir_analysis} participant '\
                    f'--participant-label sub-{sub} '\
                f'--skip-bids-validation '\
                f'--output-spaces func fsnative fsaverage T1w MNI152NLin2009cAsym '\
                f'--dummy-scans 0 '\
                f'--use-syn-sdc '\
                f'--fs-license-file /license/license.txt '\
                f'--nthreads {nthreads} '\
                f'--omp-nthreads {nthreads} '\
                f'--stop-on-first-crash '\
                f'--mem_mb {(mem*1000)-5000} '\
                
            if run_lc:
                sp.run(cmd,shell=True)
    return

def launch_prf(lc_config, run_lc):    
    
    # gathering the input for cmd
    basedir=lc_config['general']['basedir']
    container= lc_config['general']['container']
    sif_path=  lc_config['general']['sif_path']  
    homedir=os.path.join(basedir, 'singularity_home')  
    
    cmd= f'module unload singularity/3.5.2; '\
                f'module load singularity/3.5.2; '\
                f'unset PYTHONPATH; ' \
                f'singularity run '\
                f'-H {homedir} '\
                f'-B {basedir}/derivatives/fmriprep:/flywheel/v0/input '\
                f'-B {basedir}/derivatives:/flywheel/v0/output '\
                f'-B {basedir}/BIDS:/flywheel/v0/BIDS '\
                    f'-B {basedir}/container_specific_config/{container}.json:/flywheel/v0/config.json '\
	            f'-B {basedir}/license/license.txt:/opt/freesurfer/.license '\
                f'--cleanenv {sif_path} '\
    
    if run_lc:
        sp.run(cmd, shell=True)
    
    return
# %% main()
def main():

    # function 1
    do.setup_logger()
    parser_namespace, parser_dict = do.get_parser()
    
    #get the path from commandline input
    lc_config_path = parser_namespace.lc_config
    lc_config = do.read_yaml(lc_config_path)
    
    container= lc_config['general']['container']
    sub_ses_list_path = parser_namespace.sub_ses_list
    sub_ses_list = do.read_df(sub_ses_list_path)
    
    container_specific_config_path = parser_dict["container_specific_config"]#this is a list 
    
    # stored value
    run_lc = parser_namespace.run_lc
    verbose=parser_namespace.verbose

    #set the logging level to get the command    
    print_command_only=lc_config["general"]["print_command_only"] #TODO this should be defiend using -v and -print command only
    # set logger message level TODO: this should be implememt to be changeable for future 
    if print_command_only:    
        logger.setLevel(logging.CRITICAL)
    
    if verbose:
        logger.setLevel(logging.INFO)    

    if container in ['anatrois', 'rtppreproc', 'rtp-pipeline']:
        config_under_analysis, Dir_analysis=prepare.prepare_dwi_input(parser_namespace, lc_config, sub_ses_list)
        new_lc_config=do.read_yaml(config_under_analysis["new_lc_config_path"])
        new_sub_ses_list=do.read_df(config_under_analysis["new_sub_ses_list_path"])
        launch_dwi(Dir_analysis,new_lc_config, new_sub_ses_list, config_under_analysis, run_lc)

    if container == 'fmriprep':
        launch_fmriprep(Dir_analysis,lc_config,sub_ses_list, run_lc)
    if container in ['prfprepare', 'prfanalyze', 'prfreport']:
        if prepare.prepare_prf():
            launch_prf(lc_config, run_lc)
    
# #%%
if __name__ == "__main__":
    main()
