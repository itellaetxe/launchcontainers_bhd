import logging
import createsymlinks as csl
import os
import filecmp
import utils as do
import numpy as np 
import os.path as path
import json
from os import rename
from glob import glob
from os import path, symlink, unlink
from scipy.io import loadmat


logger=logging.getLogger("GENERAL")
#%% copy configs or create new analysis
def prepare_analysis_folder(parser_namespace, lc_config):
    '''
    this function is the very very first step of everything, it is IMPORTANT, 
    it will provide a check if your desired analysis has been running before
    and it will help you keep track of your input parameteres so that you know what you are doing in your analysis    

    the option force will not be useful at the analysis_folder level, if you insist to do so, you need to delete the old analysis folder by hand
    
    after determing the analysis folder, this function will copy your input configs to the analysis folder, and it will read only from there
    '''
    # read parameters from lc_config
    
    basedir = lc_config['general']['basedir']
    container = lc_config['general']['container']
    force = lc_config["general"]["force"]
    host=lc_config['general']['host']
    analysis_num=0
    found_analysis_dir=False
    run_lc = parser_namespace.run_lc
    
    force= force and (~run_lc)    
    
    version = lc_config["container_specific"][container]["version"]    
    # Get the input config files from parser  
    original_files = [parser_namespace.lc_config, parser_namespace.sub_ses_list] + parser_namespace.container_specific_config
    

    # check: if the analysis folder is already exit
        # if it is exit, check if the config information of lc_yaml, the looping information of subseslist and contianer specific config are the smae
        # if either one of them have any tiny mistake, make a new analysis folder, and copy them to there, and give a note: this is new thing, different from 
        # what you are indicating, we add a new thing for your
    while not found_analysis_dir and analysis_num <100 and ~run_lc:
        Dir_analysis = os.path.join(
        basedir,
        "nifti",
        "derivatives",
        f"{container}_{version}",
        f"analysis-{analysis_num:02d}",
                )
        #analysis= lc_config['general']['analysis']  leandro mentioned the commantary thing
        analysis_num += 1
        # Naming the potential exist config files
        path_to_analysis_lc_config = os.path.join(Dir_analysis, "lc_config.yaml")
        path_to_analysis_sub_ses_list = os.path.join(Dir_analysis, "subSesList.txt")
        path_to_analysis_container_specific_config = [os.path.join(Dir_analysis, "config.json")]
    
        copies = [path_to_analysis_lc_config, path_to_analysis_sub_ses_list] + path_to_analysis_container_specific_config
    
        all_copies_present= all(os.path.isfile(copy_path) for copy_path in copies)


        if os.path.isdir(Dir_analysis):
            found_analysis_dir=True
            
            if all_copies_present:
                # compare if all the diles are the same
                general_input= lc_config["general"]
                container_input=lc_config["container_specific"][container]
                host_input= host

                lc_config_copy=do.read_yaml(path_to_analysis_lc_config)
                container_ana= lc_config_copy['general']['container']
                host_ana=lc_config_copy['general']['host']
                
                general_copy= lc_config_copy["general"]
                container_copy=lc_config_copy["container_specific"][container_ana]
                host_copy= host_ana
                
                
                compare_config_yaml= (general_input==general_copy) and (container_input==container_copy) and (host_input==host_copy)

                are_they_same = all(filecmp.cmp(orig, copy, shallow=False)
                                    for orig, copy in zip(original_files[1:], copies[1:])) and compare_config_yaml
                # if the config info are all the same, we didn't create new analysis folder
                if are_they_same:
                    logger.warning("\n"
                                    + f"the config files in {Dir_analysis} are the same as your input, remain old filesif you are confident to run, type --run_lc flag")
                    #we found the same one so we are not going to make new analysis
                    pass
                else:
                    logger.info("\n"
                                + f"the config files in {Dir_analysis} are NOT the same as your input create new analysis folder"
                                + f"going to create analysis {analysis_num:02}")


            if not all_copies_present:
                logger.info(("\n"
                            + f"some of the config files in {Dir_analysis} missing, create a new one {analysis_num:02}"))

                
        # if it is not exit, we are doing new analysis, so we just create the analysis folder as it indicate in the config.yaml
        if not os.path.isdir(Dir_analysis):
            logger.info("\n"
                        + f"the {Dir_analysis} are not exist, making the analysis folder {analysis_num:02}")

    Dir_analysis = os.path.join(
        basedir,
        "nifti",
        "derivatives",
        f"{container}_{version}",
        f"analysis-{analysis_num:02d}",
                )
             
    return  Dir_analysis
# %% prepare_input_files
def prepare_dwi_input(parser_namespace, lc_config, df_subSes):
    """

    Parameters
    ----------
    lc_config : TYPE
        DESCRIPTION.
    df_subSes : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """
    logger.info("\n"+
                "#####################################################\n"
                +"---starting to preprare the input files for analysis\n")
    
    container = lc_config["general"]["container"]
    version = lc_config["container_specific"][container]["version"]
    Dir_analysis = prepare_analysis_folder(parser_namespace, lc_config)
    
    # first thing, if the container specific config is not correct, then not doing anything
    if len(parser_namespace.container_specific_config)==0:
                logger.error("\n"
                              +f"Input file error: the containerspecific config is not provided")
                raise FileNotFoundError("Didn't input container_specific_config, please indicate it in your commandline flag -cc")
    
    
    for row in df_subSes.itertuples(index=True, name="Pandas"):
        sub = row.sub
        ses = row.ses
        RUN = row.RUN
        dwi = row.dwi
        
        logger.info("\n"
                    +"The current run is: \n"
                    +f"{sub}_{ses}_RUN-{RUN}_{container}_{version}\n")
        
        if RUN == "True" and dwi == "True":
            if "rtppreproc" in container:
                config_under_analysis=csl.rtppreproc(parser_namespace, Dir_analysis, lc_config, sub, ses)
            elif "rtp-pipeline" in container:
                
                if not len(parser_namespace.container_specific_config_path) == 2:
                    logger.error("\n"
                              +f"Input file error: the RTP-PIPELINE config is not provided completely")
                    raise FileNotFoundError('The RTP-PIPELINE needs the config.json and tratparams.csv as container specific configs')
                
                config_under_analysis=csl.rtppipeline(parser_namespace, Dir_analysis,lc_config, sub, ses)
            elif "anatrois" in container:
                config_under_analysis =csl.anatrois(parser_namespace, Dir_analysis,lc_config,sub, ses)
            
            # future container
            else:
                logger.error("\n"+
                             f"***An error occured"
                             +f"{container} is not created, check for typos or contact admin for singularity images\n"
                )

        else:
            continue
    logger.info("\n"+
                "#####################################################\n")
    return config_under_analysis, Dir_analysis

def fmrprep_intended_for(sub_ses_list, bids_layout):
    '''
    not imlement yet, thinkging how to smartly do the job
    '''
    layout= bids_layout
    subs= sub_ses_list['sub'].tolist()
    #number_of_topups= fmriprep_configs['number_of_topups'] # a str
    #index_of_new_topups= fmriprep_configs['number_of_topups'] # a str about the functional run 
    exp_TRs= [2] #fmriprep_configs['exp_TRs'] # a list
    
    for sub in subs:
        sess= layout.get(subject=sub, return_type='id', target='session')
        ''' to be implement: the sess now is not controlled by the sub ses list'''
        logger.info(f'\n working on {sub}...')
        for ses in sess:
    
                # load func and fmaps
                funcNiftis = layout.get(subject=sub, session=ses, extension='.nii.gz', datatype='func')
                fmapNiftis = layout.get(subject=sub, session=ses, extension='.nii.gz', datatype='fmap')

                funcNiftisMeta = [funcNiftis[i].get_metadata() for i in range(len(funcNiftis))]
                fmapNiftisMeta = [fmapNiftis[i].get_metadata() for i in range(len(fmapNiftis))]

                for res in exp_TRs:
                    funcN = np.array(funcNiftis)[[i['RepetitionTime'] == res for i in funcNiftisMeta]]
                    # fmapN = np.array(fmapNiftis)[[i['RepetitionTime'] == res for i in fmapNiftisMeta]]
                    fmapN = fmapNiftis
                    
                    # make list with all relative paths of func
                    funcNiftisRelPaths = [path.join(*funcN[i].relpath.split("/")[1:]) for i in range(len(funcN))]
                    funcNiftisRelPaths = [fp for fp in funcNiftisRelPaths if ((fp.endswith('_bold.nii.gz') or 
                                                                            fp.endswith('_sbref.nii.gz')) and 
                                                                            all([k not in fp for k in ['mag', 'phase']]))]

                    # add list to IntendedFor field in fmap json
                    for fmapNifti in fmapN:
                        if not path.exists(fmapNifti.filename.replace('.nii.gz', '_orig.json')):
                            f = fmapNifti.path.replace('.nii.gz', '.json')

                            with open(f, 'r') as file:
                                j = json.load(file)

                            j['IntendedFor'] = [f.replace("\\", "/") for f in funcNiftisRelPaths]

                            rename(f, f.replace('.json', '_orig.json'))

                            with open(f, 'w') as file:
                                json.dump(j, file, indent=2)
        
    '''add a function to check, if all the intended for is here, if so, return fmriprep'''
    return True
# on going :
def prepare_fmriprep_input(sub_ses_list, bidslayout, fmriprep_configs):
    # first create intendedfor at the fmap, topup files
    # then input the subject looping thing to the fmriprep cmd command
    # not implement
    
    return
def link_vistadisplog(sub_ses_list, bids_layout):
    
    tasks= bidslayout.gettask() # a list
    taskdict=  {}
    for index, item in enumerate(tasks):
        taskdict[item]=index+1
    
    for row in sub_ses_list.itertuples(index=True, name='Pandas'):
        sub  = row.sub
        ses  = row.ses
        RUN  = row.RUN
        matFiles = np.sort(glob(path.join(baseP, sub, ses, '20*.mat')))
        for matFile in matFiles:

            stimName = loadmat(matFile, simplify_cells=True)['params']['loadMatrix']
            print(stimName)
            for key in taskdict.keys():
                
                if key in stimName:
                    if 'tr-2' in stimName:
                        linkName = path.join(path.dirname(matFile), f'{sub}_{ses}_task-ESCB_run-0{CB}_params.mat')
                        taskdict[key] += 1

                if path.islink(linkName):
                    unlink(linkName)

                symlink(path.basename(matFile), linkName)

    return True
def prepare_prf_input(container, sub_ses_list, bids_layout ,run_lc):
    # first prepare the sourcedata, the vistadisp-log
    # then write the subject information to the config.json file
    
    if not run_lc:
        # if the container is prfprepare, do the preparation for vistadisplog
        # copy the container specific information to the prfprepare.json.
        # copy the information in subseslist to the prfprepare.json
        # question, in this way, do we still need the config.json???
            # i mean yes, but there will always to better options
        sub_list=sub_ses_list[sub_ses_list['RUN']]['sub'].tolist()
        ses_list=sub_ses_list[sub_ses_list['RUN']]['ses'].tolist()
        config_name=f'{container}.json'
        with open(config_name, 'r') as config_json:
            j= json.load(config_json)
        
        if continer == 'prfprepare':   
            # do i need to add a check here? I don't think so
            if link_vistadisplog(sub_ses_list, bids_layout):
                logger.info('\n'
                + f'the {container} prestep link vistadisplog has been done!')
                j['subjects'] =' '.join(sub_list)
                j['sessions'] =' '.join(ses_list)

        if container =='prfresult':    
            j['subjects'] =' '.join(sub_list)
            j['sessions'] =' '.join(ses_list)
        if container == 'prfanalyze-vista':
            j['subjectName'] =' '.join(sub_list)
            j['sessionName'] =' '.join(ses_list)
        rename(config_name, config_name.replace('.json','_orig.json'))
        
        with open(config_name, 'w') as config_json:
            json.dump(j, config_json, indent=2)
    return