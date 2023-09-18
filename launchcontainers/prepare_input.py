import logging
import createsymlinks as csl


logger=logging.getLogger("GENERAL")
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
        
        if RUN == "True":
            if container_specific_config_path is None:
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

# %%
