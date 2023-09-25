import os
import subprocess as sp
import numpy as np
import logging
from dask import progress
# modules in lc
import dask_schedule_queue as dsq
import prepare_input as prepare
import utils as do


logger=logging.getLogger("GENERAL")

# %% launchcontainers
def generate_cmd(new_lc_config,sub,ses,Dir_analysis, new_configFilePath_dict):
    
    container= new_lc_config['general']['container']
    host = new_lc_config['gnenral']['host']
    path_to_config_json= new_configFilePath_dict['container_specific_configs']
    
    if container in ['anatrois', 'rtppreproc','rtp-pipeline']:
        logger.info("\n"+ f'start to generate the DWI PIPELINE command')
        logdir= os.path.join(
                Dir_analysis,
                "sub-" + sub,
                "ses-" + ses,
                "output", "log"
            )
        logfilename=f"{logdir}/t-{container}-sub-{sub}_ses-{ses}"
        path_to_sub_derivatives=os.path.join(Dir_analysis,
                                        f"sub-{sub}",
                                        f"ses-{ses}")
        
        if ("BCBL" == host) or ("local" == host):
            cmd=f"singularity run -e --no-home "\
                f"--bind /bcbl:/bcbl "\
                f"--bind /tmp:/tmp "\
                f"--bind /export:/export "\
                f"--bind {path_to_sub_derivatives}/input:/flywheel/v0/input:ro "\
                f"--bind {path_to_sub_derivatives}/output:/flywheel/v0/output "\
                f"--bind {path_to_config_json}:/flywheel/v0/config.json "\
                f"{sif_path} 2>> {logfilename}.e 1>> {logfilename}.o "
        
        elif "DIPC" == host:
            cmd=f"singularity run -e --no-home "\
                f"--bind /scratch:/scratch "\
                f"--bind {path_to_sub_derivatives}/input:/flywheel/v0/input:ro "\
                f"--bind {path_to_sub_derivatives}/output:/flywheel/v0/output "\
                f"--bind {path_to_config_json}:/flywheel/v0/config.json "\
                f"{sif_path} 2>> {logfilename}.e 1>> {logfilename}.o "
    if container == 'fmriprep':
        logger.info("\n"+ f'start to generate the FMRIPREP command')
        
        basedir=new_lc_config['general']['basedir']
        homedir= os.path.join(basedir,'singualrity_home')
        
        nthreads=new_lc_config[container]['nthreads']
        mem=new_lc_config[container]['mem'] 
        fs_lisense=new_lc_config[container]['fs_lisense']
        sif_path=  new_lc_config['general']['sif_path']  
        precommand=f'module unload singularity/3.5.2; '\
                f'module load singularity/3.5.2; '\
                f'mkdir -p {homedir}; '\
                f'unset PYTHONPATH; ' \
        
        # so this code, the cmd code for bcbl, dipc, is the same, all this function is to have a runned sp.run, so that the dask can work 
        cmd=  f'singularity run '\
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
    
    if container in ['prfprepare','prfreport','prfanalyze-vista']:
        cmd= f'singularity run '\
                f'-H {homedir} '\
                f'-B {basedir}/derivatives/fmriprep:/flywheel/v0/input '\
                f'-B {basedir}/derivatives:/flywheel/v0/output '\
                f'-B {basedir}/BIDS:/flywheel/v0/BIDS '\
                    f'-B {basedir}/container_specific_config/{container}.json:/flywheel/v0/config.json '\
	            f'-B {basedir}/license/license.txt:/opt/freesurfer/.license '\
                f'--cleanenv {sif_path} '\
    
    if cmd is None:
        logger.error("\n"+ f'the DWI PIPELINE command is not assigned, please check your config.yaml[general][host] session\n')
        raise ValueError('cmd is not defiend, aborting')
    
    return cmd
 
#%% the launchcontainer
def launchcontainer(Dir_analysis, new_lc_config, sub_ses_list, new_configFilePath_dict, run_lc):
    """
    This function launches containers generically in different Docker/Singularity HPCs
    This function is going to assume that all files are where they need to be.

    Parameters
    ----------

  
   
   """
    logger.info("\n"+
                "#####################################################\n")

    host = new_lc_config["general"]["host"]
    jobqueue_config= new_lc_config["host_options"][host]
    
    force = new_lc_config["general"]["force"]    

    new_lc_configs=[]
    subs=[]
    sess=[]
    Dir_analysiss=[]
    new_configFilePath_dicts=[]
    
    for row in sub_ses_list.itertuples(index=True, name='Pandas'):
        sub  = row.sub
        ses  = row.ses
        RUN  = row.RUN
        dwi  = row.dwi
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



            path_to_config_json = new_configFilePath_dict["new_container_specific_config_path"]
            path_to_config_yaml = new_configFilePath_dict["new_lc_config_path"]
            path_to_subSesList  = new_configFilePath_dict["new_sub_ses_list_path"]


            

            if not os.path.isdir(tmpdir):
                os.mkdir(tmpdir)
            if not os.path.isdir(logdir):
                os.mkdir(logdir)
            if not os.path.isdir(backup_configs):
                os.mkdir(backup_configs)
            
            backup_config_json = os.path.join(backup_configs, "config.json")
            backup_config_yaml = os.path.join(backup_configs, "config_lc.yaml")
            backup_subSesList = os.path.join(backup_configs, "subSesList.txt")
            
            new_lc_configs.append(new_lc_config)
            subs.append(sub)
            sess.append(ses)
            Dir_analysiss.append(Dir_analysis)
            new_configFilePath_dicts.append(new_configFilePath_dict)
            
            if not run_lc:
                # this cmd is only for print the command 
                command= generate_cmd(new_lc_config,sub,ses,Dir_analysis, new_configFilePath_dict)
                logger.critical("\n"
                                    +f"--------run_lc is false, if True, we would launch this command: \n"
                                    +f"\n this command will be run on the {host}\n"
                                    +f"{command}\n\n"
                                    +"Please check if the job_script is properlly defined and then starting run_lc \n")
    if run_lc:

        # Count how many jobs we need to launch from  sub_ses_list
        n_jobs = np.sum(sub_ses_list.RUN == "True")

        client, cluster = dsq.dask_scheduler(jobqueue_config,n_jobs)
        logger.info("---this is the cluster and client\n"
                    +f"{client} \n cluster: {cluster} \n")
        futures = client.map(sp.run(generate_cmd,shell=True),new_lc_configs,subs,sess,Dir_analysiss,new_configFilePath_dicts)
        progress(futures)
        results = client.gather(futures)
        logger.ino(results)
        logger.ino('###########')
        client.close()
        cluster.close()

        
        do.copy_file(path_to_config_json,backup_config_json,force)
        do.copy_file(path_to_config_yaml, backup_config_yaml, force)
        do.copy_file(path_to_subSesList, backup_subSesList, force)
        logger.info("\n"
                    +f"copied all the config files to {backup_configs} folder")
        logger.critical("\n"
                         + "lanchcontainer finished, all the jobs are done")
    return
# %% main()
def main():
    #get the path from commandline input
    parser_namespace = do.get_parser()
    lc_config_path = parser_namespace.lc_config
    lc_config = do.read_yaml(lc_config_path)   
    container= lc_config['general']['container']
    sub_ses_list_path = parser_namespace.sub_ses_list
    sub_ses_list = do.read_df(sub_ses_list_path)    
    
    # stored value
    run_lc = parser_namespace.run_lc
    verbose=parser_namespace.verbose
    debug= parser_namespace.DEBUG
    
    #set the logging level to get the command
    do.setup_logger()    
    print_command_only=lc_config["general"]["print_command_only"] #TODO this should be defiend using -v and -print command only
    # set logger message level TODO: this should be implememt to be changeable for future 
    if print_command_only:    
        logger.setLevel(logging.CRITICAL)
    
    if verbose:
        logger.setLevel(logging.INFO)    
    if debug:
        logger.setLevel(logging.DEBUG)
    
    # prepare file and launch containers
    # for DWI pipeline
    if container in ['anatrois', 'rtppreproc', 'rtp-pipeline']:
        new_configFilePath_dict, Dir_analysis = prepare.prepare_dwi_input(parser_namespace, lc_config, sub_ses_list)
    
    if container == 'fmriprep':
        new_configFilePath_dict, Dir_analysis = prepare.prepare_fmriprep_input(parser_namespace, lc_config, sub_ses_list)
    
    if container in ['prfprepare', 'prfanalyze', 'prfreport']:
        pass  
    
    new_lc_config=do.read_yaml(new_configFilePath_dict["new_lc_config_path"])
    new_sub_ses_list=do.read_df(new_configFilePath_dict["new_sub_ses_list_path"])
    
    launchcontainer(Dir_analysis, new_lc_config , new_sub_ses_list, run_lc)
    

    
# #%%
if __name__ == "__main__":
    main()
