import argparse
from argparse import RawDescriptionHelpFormatter

import sys


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
        Before you make use of this program, please prepare the environment, edit the required config files, to match your analysis demand. \n
        SAMPLE CMD LINE COMMAND \n\n
        ###########STEP1############# \n
        To begin the analysis, you need to first prepare and check the input files by typing this command in your bash prompt:
        python path/to/the/launchcontianer.py -lcc path/to/launchcontainer_config.yaml -ssl path/to/subject_session_info.txt 
        -cc path/to/container_specific_config.json \n
        ##--cc note, for the case of rtp-pipeline, you need to input two paths, one for config.json and one for tractparm.csv \n\n
        ###########STEP2############# \n
        After you have done step 1, all the config files are copied to BIDS/sub/ses/analysis/ directory 
        When you are confident everything is there, press up arrow to recall the command in STEP 1, and just add --run_lc after it. \n\n  
        
        We add lots of check in the script to avoid program breakdowns. if you found new bugs while running, do not hesitate to contact us"""
    , formatter_class=RawDescriptionHelpFormatter)

    parser.add_argument(
        "-lcc",
        "--lc_config",
        type=str,
        #default="",
        help="path to the config file",
    )
    parser.add_argument(
        "-ssl",
        "--sub_ses_list",
        type=str,
        #default="",
        help="path to the subSesList",
    )
    parser.add_argument(
        "-cc",
        "--container_specific_config",
        nargs='*',
        default=[],
        #default=["/export/home/tlei/tlei/PROJDATA/TESTDATA_LC/Testing_02/BIDS/config.json"],
        help="path to the container specific config file(s). First file needs to be the config.json file of the container. \
        Some containers might need more config files (e.g., rtp-pipeline needs tractparams.csv). \
        some don't need any configs (e.g fmriprep)    Add them here separated with a space.",
    )
   
    parser.add_argument('--run_lc', action='store_true',
                        help= "if you type --run_lc, the entire program will be launched, jobs will be send to \
                        cluster and launch the corresponding container you suggest in config_lc.yaml. \
                        We suggest that the first time you run launchcontainer.py, leave this argument empty. \
                        then the launchcontainer.py will prepare \
                        all the input files for you and print the command you want to send to container, after you \
                        check all the configurations are correct and ready, you type --run_lc to make it run"
                        )
    
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="if you want to open verbose mode, type -v or --verbose, other wise the program is non-verbose mode",
                         )
    parser.add_argument(
        "--DEBUG",
        action="store_true",
        help="if you want to find out what is happening of particular step, this will print you more detailed information",
                         )    
    if len(sys.argv)==1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    
    parse_dict = vars(parser.parse_args())
    parse_namespace= parser.parse_args()
    
    print("\n"+
        "#####################################################\n" +
        "This is the result from get_parser()\n"+
                f'{parse_dict}\n'+    
        "#####################################################\n")
    
    return parse_namespace

# %% parser
def get_parser2():
    """
    Input:
    Parse command line inputs

    Returns:
    a dict stores information about the cmd input

    """
    parser = argparse.ArgumentParser(
        description= """
        This python program helps you analysis MRI data through different containers,
        Before you make use of this program, please prepare the environment, edit the required config files, to match your analysis demand. \n
        SAMPLE CMD LINE COMMAND \n\n
        ###########STEP1############# \n
        To begin the analysis, you need to first prepare and check the input files by typing this command in your bash prompt:
        python path/to/the/launchcontianer.py -lcc path/to/launchcontainer_config.yaml -ssl path/to/subject_session_info.txt 
        -cc path/to/container_specific_config.json \n
        ##--cc note, for the case of rtp-pipeline, you need to input two paths, one for config.json and one for tractparm.csv \n\n
        ###########STEP2############# \n
        After you have done step 1, all the config files are copied to BIDS/sub/ses/analysis/ directory 
        When you are confident everything is there, press up arrow to recall the command in STEP 1, and just add --run_lc after it. \n\n  
        
        We add lots of check in the script to avoid program breakdowns. if you found new bugs while running, do not hesitate to contact us"""
    , formatter_class=RawDescriptionHelpFormatter)

    parser.add_argument(
        "-lcc",
        "--lc_config",
        type=str,
        #default="",
        help="path to the config file",
    )
    parser.add_argument(
        "-ssl",
        "--sub_ses_list",
        type=str,
        #default="",
        help="path to the subSesList",
    )
    parser.add_argument(
        "-cc",
        "--container_specific_config",
        nargs='*',

        #default=["/export/home/tlei/tlei/PROJDATA/TESTDATA_LC/Testing_02/BIDS/config.json"],
        help="path to the container specific config file(s). First file needs to be the config.json file of the container. \
        Some containers might need more config files (e.g., rtp-pipeline needs tractparams.csv). \
        some don't need any configs (e.g fmriprep)    Add them here separated with a space.",
    )
   
    parser.add_argument('--run_lc', action='store_true',
                        help= "if you type --run_lc, the entire program will be launched, jobs will be send to \
                        cluster and launch the corresponding container you suggest in config_lc.yaml. \
                        We suggest that the first time you run launchcontainer.py, leave this argument empty. \
                        then the launchcontainer.py will prepare \
                        all the input files for you and print the command you want to send to container, after you \
                        check all the configurations are correct and ready, you type --run_lc to make it run"
                        )
    
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="if you want to open verbose mode, type -v or --verbose, other wise the program is non-verbose mode",
                         )
    parser.add_argument(
        "--DEBUG",
        action="store_true",
        help="if you want to find out what is happening of particular step, this will print you more detailed information",
                         )    
    if len(sys.argv)==1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    
    parse_dict = vars(parser.parse_args())
    parse_namespace= parser.parse_args()
    
    print("\n"+
        "#####################################################\n" +
        "This is the result from get_parser2222()\n"+
                f'{parse_dict}\n'+    
        "#####################################################\n")
    
    return parse_namespace


def main():

    
    #get the path from command line input
    #parser_namespace = get_parser()
    parser_namespace= get_parser2()
    print(parser_namespace.container_specific_config)
    print(parser_namespace)

    
# #%%
if __name__ == "__main__":
    main()
