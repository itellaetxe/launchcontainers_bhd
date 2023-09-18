import argparse
from argparse import RawDescriptionHelpFormatter
import yaml
from yaml.loader import SafeLoader
import logging
import pandas as pd
import os
import zipfile
import shutil

logger=logging.getLogger("GENERAL")

# %% parser
def get_parser():
    """
    Input:
    Parse command line inputs

    Returns:
    a dict stores information about the cmd input

    """
    parser = argparse.ArgumentParser(
        description= """
        This python program helps you analysis MRI data through different containers,
        Before you make use of this program, please edit the required config files to match your analysis demand. \n
        SAMPLE CMD LINE COMMAND \n\n
        ###########STEP1############# \n
        To begin the analysis, you need to first prepare and check the input files by typing this command in your bash prompt:
        python path/to/the/launchcontianer.py -lcc path/to/launchcontainer_config.yaml -ssl path/to/subject_session_info.txt 
        -cc path/to/contianer_specific_config.json \n
        ##--cc note, for the case of rtp-pipeline, you need to input two paths, one for config.json and one for tractparm.csv \n\n
        ###########STEP2############# \n
        After you have done step 1, all the config files are copied to nifti/sub/ses/analysis/ directory 
        When you are confident everthing is there, press up arrow to recall the command in STEP 1, and just add --run_lc after it. \n\n  
        
        We add lots of check in the script to avoid program breakdowns. if you found new bugs while running, do not hesitate to contact us"""
    , formatter_class=RawDescriptionHelpFormatter)

    parser.add_argument(
        "-lcc",
        "--lc_config",
        type=str,
        # default="/Users/tiger/TESTDATA/PROJ01/nifti/config_launchcontainer_copy.yaml",
        default="/export/home/tlei/tlei/PROJDATA/TESTDATA_LC/Testing_02/nifti/lc_config.yaml",
        help="path to the config file",
    )
    parser.add_argument(
        "-ssl",
        "--sub_ses_list",
        type=str,
        # default="/Users/tiger/TESTDATA/PROJ01/nifti/subSesList.txt",
        default="/export/home/tlei/tlei/PROJDATA/TESTDATA_LC/Testing_02/nifti/subSesList.txt",
        help="path to the subSesList",
    )
    parser.add_argument(
        "-cc",
        "--container_specific_config",
        nargs='+',
        # default="/Users/tiger/Documents/GitHub/launchcontainers/example_configs/container_especific_example_configs/anatrois/4.2.7_7.1.1/example_config.json",
        help="path to the container specific config file(s). First file needs to be the config.json file of the container. Some containers might need more config files (e.g., rtp-pipeline needs tractparams.csv). Add them here separated with a space.",
    )
   
    parser.add_argument('--run_lc', action='store_true',
                        help= "if you type --run_lc, the entire program will be launched, jobs will be send to \
                        cluster and launch the corresponding container you suggest in config_lc.yaml. \
                        We suggest that the first time you run launchcontainer.py, leave this arguement empty. \
                        then the launchcontainer.py will preapre \
                        all the input files for you and print the command you want to send to container, after you \
                        check all the configurations are correct and ready, you type --run_lc to make it run"
                        )
    parser.add_argument('--not_run_lc', dest='run_lc', action='store_false')
    #parser.set_defaults(run_lc=False)
    
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="if you want to open verbose mode, type -v or --verbose, other wise the program is non-verbose mode",
                         )
    
    parse_dict = vars(parser.parse_args())
    parse_namespace= parser.parse_args()

    logger.info("\n"+
        "#####################################################\n" +
        "This is the result from get_parser()\n"+
                f'{parse_dict}\n'+    
        "#####################################################\n")
    
    return parse_namespace ,parse_dict
# %% read yaml
def read_yaml(path_to_config_file):
    """
    Input:
    the path to the config file

    Returns
    a dictionary that contains all the config info

    """
    with open(path_to_config_file, "r") as v:
        config = yaml.load(v, Loader=SafeLoader)

    container = config["general"]["container"]

    logger.info("\n"+
            "#####################################################\n"
            +f"Successfully read the config file {path_to_config_file} \n"
            +f'Basedir is: {config["general"]["basedir"]} \n'
            +f'Container is: {container}_{config["container_specific"][container]["version"]} \n'
            +f'Analysis is: analysis-{config["general"]["analysis"]} \n'
            +f"##################################################### \n")

    return config

# %% function to read subSesList. txt
def read_df(path_to_df_file):
    """
    Input:
    path to the subject and session list txt file

    Returns
    a dataframe

    """
    outputdf = pd.read_csv(path_to_df_file, sep=',',dtype=str)
    num_rows = len(outputdf)

    # Print the result    
    logger.info("\n"+
        "#####################################################\n" +
        f"The dataframe{path_to_df_file} is successfully read\n"+
                f"The DataFrame has {num_rows} rows \n"+    
        "#####################################################\n")

    return outputdf

#%% function setup_logger
def setup_logger():
    # instantiate logger
    logger=logging.getLogger()
    # define handler and formatter
    handler = logging.StreamHandler() #TODO: this shoul dbe implement to filehandler also , so that we have lc logs
    formatter= logging.Formatter("%(asctime)s %(levelname)8s  %(module)8s:%(funcName)s:%(lineno)d %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
    # add formatter to handler
    handler.setFormatter(formatter)
    # add handler to logger
    logger.addHandler(handler)

    return logger
#%% copy file
def copy_file(src_file, dst_file, force):
    logger.info("\n"+
        "#####################################################\n")
    if not os.path.isfile(src_file):
        logger.error(" An error occurred")
        raise FileExistsError("the source file is not here")
    
    logger.info("\n"+
                f"---start copying {src_file} to {os.path.dirname(dst_file)} \n")
    try:
        if ((not os.path.isfile(dst_file)) or (force)) or (os.path.isfile(dst_file) and force):
            shutil.copy(src_file, dst_file)
            logger.info("\n"+
                f"---{src_file} has been succesfully copied to derivatives/analysis directory \n"+
                f"---REMEMBER TO CHECK/EDIT TO HAVE THE CORRECT PARAMETERS IN THE FILE\n"
            )
        elif os.path.isfile(dst_file) and not force:
            logger.warning("\n"+
                    f"---copy are not operating, the {src_file} already exist")


    # If source and destination are same
    except shutil.SameFileError:
        logger.error("***Source and destination represents the same file.\n")
        raise
    # If there is any permission issue
    except PermissionError:
        logger.error("***Permission denied.\n")
        raise
    # For other errors
    except:
        logger.error("***Error occurred while copying file\n")
        raise
    logger.info("\n"+
        "#####################################################\n")
    
    return dst_file
#%% check if tractparam ROI was created in the anatrois fs.zip file
def check_tractparam(lc_config, sub, ses, tractparam_df):
    """

        Parameters
        ----------
        lc_config : dict
             the config info about lc
        sub : str
        ses: str
        tractparam_df : dataframe

            inherited parameters: path to the fs.zip file
                defined by lc_config, sub, ses
        Returns
        -------
        None.
    """
    # Define the list of required ROIs
    logger.info("\n"+
                "#####################################################\n")
    required_rois=set()
    for col in ['roi1', 'roi2', 'roi3', 'roi4',"roiexc1","roiexc2"]:
        for val in tractparam_df[col][~tractparam_df[col].isna()].unique():
            if val != "NO":
                required_rois.add(val)

    # Define the zip file
    basedir = lc_config["general"]["basedir"]
    container = lc_config["general"]["container"]
    precontainerfs = lc_config["container_specific"][container]["precontainerfs"]
    preanalysisfs = lc_config["container_specific"][container]["preanalysisfs"]
    fs_zip = os.path.join(
        basedir,
        "nifti",
        "derivatives",
        precontainerfs,
        "analysis-" + preanalysisfs,
        "sub-" + sub,
        "ses-" + ses,
        "output", "fs.zip"
    )
    # Extract .gz files from zip file and check if they are all present
    with zipfile.ZipFile(fs_zip, 'r') as zip:
        zip_gz_files = set(zip.namelist())
    required_gz_files = set(f"fs/ROIs/{file}.nii.gz" for file in required_rois)
    logger.info("\n"
                +f"---The following are the ROIs in fs.zip file: \n {zip_gz_files} \n"
                +f"---there are {len(zip_gz_files)} .nii.gz files in fs.zip from anarois output\n"
                +f"---There are {len(required_gz_files)} ROIs that are required to run RTP-PIPELINE\n")
    if required_gz_files.issubset(zip_gz_files):
        logger.info("\n"
                +"---checked! All required .gz files are present in the fs.zip \n")
    else:
        missing_files = required_gz_files - zip_gz_files
        logger.error("\n"
                     +f"*****Error: \n"
                     +f"there are {len(missing_files)} missed in fs.zip \n"
                     +f"The following .gz files are missing in the zip file:\n {missing_files}")
        raise FileNotFoundError("Required .gz file are missing")
    
    ROIs_are_there= required_gz_files.issubset(zip_gz_files)
    logger.info("\n"+
                "#####################################################\n")
    return ROIs_are_there

#%%
def main():
    parser_namespace, parser_dict = get_parser()
    
    print(parser_dict['lc_config'])
    print("this is : \n",parser_namespace.container_specific_config)

    lc_config_path = parser_namespace.lc_config
    lc_config =read_yaml(lc_config_path)

    print_command_only=lc_config["general"]["print_command_only"]

    print(print_command_only)

    container_specific_config_path = parser_dict["container_specific_config"]
    print(container_specific_config_path)
    '''
    if parser_namespace.verbose:  
        print("verbose is on")
    else:
        print("verbose is OFF")
    
    print("\n", parser_namespace)
'''
if __name__ == "__main__":
    setup_logger()
    logger.setLevel(logging.INFO)
    main() 





    