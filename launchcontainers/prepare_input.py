import logging
import createsymlinks as csl
import os
import filecmp
logger=logging.getLogger("GENERAL")

#%% copy configs or create new analysis
def create_analysis_folder(parser_namespace, lc_config):
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
    analysis =  lc_config['general']['analysis']
    force = lc_config["general"]["force"]
    run_lc = parser_namespace.run_lc
    force= force and (~run_lc)
    version = lc_config["container_specific"][container]["version"] 
    
    Dir_analysis = os.path.join(
            basedir,
            "nifti",
            "derivatives",
            f"{container}_{version}",
            "analysis-" + analysis,
        )
    # Get the input config files from parser
    original_files = [parser_namespace.lc_config, parser_namespace.sub_ses_list] + parser_namespace.container_specific_config
    
    # Naming the potential exist config files
    path_to_analysis_lc_config = os.path.join(Dir_analysis, "lc_config.yaml")
    path_to_analysis_sub_ses_list = os.path.join(Dir_analysis, "subSesList.txt")
    path_to_analysis_container_specific_config = [os.path.join(Dir_analysis, "config.json")]
    
    copies = [path_to_analysis_lc_config, path_to_analysis_sub_ses_list] + path_to_analysis_container_specific_config
    
    all_copies_present= all(os.path.isfile(copy_path) for copy_path in copies)
    # check: if the analysis folder is already exit
        # if it is exit, check if the config information of lc_yaml, the looping information of subseslist and contianer specific config are the smae
        # if either one of them have any tiny mistake, make a new analysis folder, and copy them to there, and give a note: this is new thing, different from 
        # what you are indicating, we add a new thing for your
    
    if os.path.isdir(Dir_analysis):
        if all_copies_present:
            # compare if all the diles are the same 
            are_they_same = all(filecmp.cmp(orig, copy, shallow=False) for orig,copy in zip(original_files, copies))
            if are_they_same:
                logging.warning("\n"
                     +f"the config files in {Dir_analysis} are the same as your input, remain old filesif you are confident to run, type --run_lc flag")
            else:
                logging.info(("\n"
                     +f"the config files in {Dir_analysis} are NOT the same as your input create new analysis folder")) 
                analysis+=1
                Dir_analysis= os.path.join(basedir,"nifti","derivatives",f"{container}_{version}", "analysis-" + analysis)
                os.mkdir(Dir_analysis)
                copy
        else:
            logging.info(("\n"
                     +f"the config files in {Dir_analysis} are NOT the same as your input create new analysis folder")) 
            analysis+=1
            Dir_analysis= os.path.join(basedir,"nifti","derivatives",f"{container}_{version}", "analysis-" + analysis)
            os.mkdir(Dir_analysis)
    
    # if it is not exit, we are doing new analysis, so we just create the analysis folder as it indicate in the config.yaml 
    else:
        logging.info("\n"
                     +f"the {Dir_analysis} are not exist, making the analysis folder as described in config.yaml")
        os.mkdir(Dir_analysis)   
    
    return  Dir_analysis
# %% prepare_input_files
def prepare_input_files(lc_config, lc_config_path, df_subSes, sub_ses_list_path, container_specific_config_path, run_lc):
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
    
    for row in df_subSes.itertuples(index=True, name="Pandas"):
        sub = row.sub
        ses = row.ses
        RUN = row.RUN
        dwi = row.dwi
        func = row.func
        container = lc_config["general"]["container"]
        version = lc_config["container_specific"][container]["version"]
        logger.info("\n"
                    +"The current run is: \n"
                    +f"{sub}_{ses}_RUN-{RUN}_{container}_{version}\n")
        
        if RUN == "True" and dwi == "True":
            if len(container_specific_config_path)==0:
                logging.error("\n"
                              +f"Input file error: the containerspecific config is not provided")
                raise FileNotFoundError("Didn't input container_specific_config, please indicate it in your commandline flag -cc")
            if "rtppreproc" in container:
                new_lc_config_path,new_sub_ses_list_path,new_container_specific_config_path=csl.rtppreproc(lc_config, lc_config_path, sub, ses, sub_ses_list_path, container_specific_config_path,run_lc)
            elif "rtp-pipeline" in container:
                if not len(container_specific_config_path) == 2:
                    logging.error("\n"
                              +f"Input file error: the RTP-PIPELINE config is not provided completely")
                    raise FileNotFoundError('The RTP-PIPELINE needs the config.json and tratparams.csv as container specific configs')
                new_lc_config_path,new_sub_ses_list_path,new_container_specific_config_path=csl.rtppipeline(lc_config,lc_config_path, sub, ses,sub_ses_list_path,container_specific_config_path,run_lc)
            elif "anatrois" in container:
                new_lc_config_path,new_sub_ses_list_path,new_container_specific_config_path =csl.anatrois(lc_config, lc_config_path,sub, ses,sub_ses_list_path, container_specific_config_path,run_lc)
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
    return new_lc_config_path, new_sub_ses_list_path,new_container_specific_config_path


