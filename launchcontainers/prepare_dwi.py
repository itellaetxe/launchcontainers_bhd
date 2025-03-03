#%% import libraries
import os, errno
import glob
import sys
import shutil
import nibabel as nib
import json
import subprocess as sp
from utils import read_df, copy_file 
import zipfile
import logging
import utils as do
logger=logging.getLogger("GENERAL")


#%% the force version of create symlink
def force_symlink(file1, file2, force):
    """
    Parameters
    ----------
    file1 : str
        the path to the source file, which is the output of previous container
    file2 : str
        the path to the destination file, which is the input of the current container
    force : bool
        set in the config file

    Raises
    ------
    e
        OS error.

    Returns
    -------
    None.

    """
    # if we don't force to overwrite
    logger.info("\n"
               +"-----------------------------------------------\n")
    if not force:
        try:
            # try the command, if the file are correct and symlink not exist, it will create one
            logger.info("\n"
                       +f"---creating symlink for source file: {file1} and destination file: {file2}\n")
            os.symlink(file1, file2)
            logger.info("\n"
                       +f"--- force is {force}, -----------------creating success -----------------------\n")
        # if raise [erron 2]: file not exist, print the error and pass
        except OSError as n:
            if n.errno == 2:
                logger.error("\n"
                             +"***An error occured \n" 
                             +"input files are missing, please check \n")
                pass
            # if raise [errno 17] the symlink exist, we don't force and print that we keep the original one
            elif n.errno == errno.EEXIST:
                logger.warning("\n"+ 
                           f"--- force is {force}, symlink exist, remain old \n")
            else:
                logger.error("\n"+ "Unknown error, break the program")
                raise n
    # if we force to overwrite
    if force:
        try:
            # try the command, if the file are correct and symlink not exist, it will create one
            os.symlink(file1, file2)
            logger.info("\n"
                       +f"--- force is {force}, symlink empty, new link created successfully\n ")
        # if the symlink exist, and in this case we force a overwrite
        except OSError as e:
            if e.errno == errno.EEXIST:
                os.remove(file2)
                logger.info("\n"
                           +f"--- force is {force}, symlink exist, unlink\n ")
                os.symlink(file1, file2)
                logger.warning("\n"
                           +"--- overwrite the existing symlink")
                logger.info("\n"
                           +"-----------------Overwrite success -----------------------\n")
            elif e.errno == 2:
                logger.error("\n"
                             +"***input files are missing, please check\n")
                raise e
            else:
                logger.error("\n"
                           +"***ERROR***\n"
                           +"We don't know what happened\n")
                raise e
    logger.info("\n"
               +"-----------------------------------------------\n")
    return
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
    bidsdir_name= lc_config["general"]["bidsdir_name"]
    version = lc_config["container_specific"][container]["version"]
    precontainer_anat = lc_config["container_specific"][container]["precontainer_anat"]
    anat_analysis_name = lc_config["container_specific"][container]["anat_analysis_name"]
    
    fs_zip = os.path.join(
        basedir,
        bidsdir_name,
        "derivatives",
        f'{precontainer_anat}',
        "analysis-" + anat_analysis_name,
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
                +f"---there are {len(zip_gz_files)} .nii.gz files in fs.zip from anatrois output\n"
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
def anatrois(parser_namespace, Dir_analysis,lc_config, sub, ses, layout):

    """
    Parameters
    ----------
    parser_namespace : parser obj
        it stores all the input information from the get parser
    lc_config : dict
        the lc_config dictionary from _read_config
    sub : str
        the subject name looping from df_subSes
    ses : str
        the session name looping from df_subSes.

    Returns
    -------
    none, create symbolic links

    """
    # define local variables from lc_config dict
    # input from get_parser
    container_specific_config_path= parser_namespace.container_specific_config
    run_lc=parser_namespace.run_lc
    
    # general level variables:
    basedir = lc_config["general"]["basedir"]
    container = lc_config["general"]["container"]    
    bidsdir_name=lc_config["general"]["bidsdir_name"]  
        # if force is False, then we don't want to overwrite anything
        # if force is true, and we didn't run_lc(in the prepare mode), we will do the overwrite and so on
        # if force is true and we do run_lc, then we will never overwrite
    force = (lc_config["general"]["force"])
    force = force and (not run_lc)
     
    # container specific:
    pre_fs = lc_config["container_specific"][container]["pre_fs"]
    prefs_zipname = lc_config["container_specific"][container]["prefs_zipname"]
    # I added this line, shall we modify config yaml
    precontainer_anat = lc_config["container_specific"][container]["precontainer_anat"]
    anat_analysis_name = lc_config["container_specific"][container]["anat_analysis_name"]
    annotfile = lc_config["container_specific"][container]["annotfile"]
    mniroizip = lc_config["container_specific"][container]["mniroizip"]
    version = lc_config["container_specific"][container]["version"]
    
    srcFile_container_config_json= container_specific_config_path[0]
    new_container_specific_config_path=[]
    
    # if we run freesurfer before:
    if pre_fs:
        logger.info("\n"
                   +f"########\n the sourceFile T1 will be pre_fs\n#########\n")
        srcAnatPath = os.path.join(
            basedir,
            bidsdir_name,
            "derivatives",
            f'{precontainer_anat}',
            "analysis-" + anat_analysis_name,
            "sub-" + sub,
            "ses-" + ses,
            "output",
        )
        zips = sorted(
            glob.glob(os.path.join(srcAnatPath, prefs_zipname + "*")), key=os.path.getmtime
        )
        logger.info("\n"
                   +f"---the len of the zip file list is {len(zips)}\n")
        if len(zips) == 0:
            logger.warning("\n"+
                f"There are no {prefs_zipname}.zip in {srcAnatPath}, we will listed potential zip file for you"
            )
            zips_new = sorted(glob.glob(os.path.join(srcAnatPath, "*")), key=os.path.getmtime)
            if len(zips_new) == 0:
                logger.error("\n"+
                    f"The {srcAnatPath} directory is empty, aborting, please check the output file of previous analysis."
                )
                raise FileNotFoundError("srcAnatPath is empty, no previous analysis was found")
            else:
                answer = input(
                    f"Do you want to use the file: \n{zips_new[-1]} \n we get for you? \n input y for yes, n for no"
                )
                if answer in "y":
                    srcFileT1 = zips_new[-1]
                else:
                    logger.error("\n"+"An error occurred"
                               +zips_new +"\n"
                               +"no target preanalysis.zip file exist, please check the config_lc.yaml file")
                    sys.exit(1)
        elif len(zips) > 1:
            logger.info("\n"
                       +f"There are more than one zip file in {srcAnatPath}, selecting the latest one")
            srcFileT1 = zips[-1]
        else:
            srcFileT1 = zips[0]

    else:
        srcFileT1_lst= layout.get(subject= sub, session=ses, extension='nii.gz',suffix= 'T1w',return_type='filename')
        if len(srcFileT1_lst) == 0:
            raise FileNotFoundError(f'the T1w.nii.gz you are specifying for sub-{sub}_ses-{ses} does NOT exist or the folder is not BIDS format, please check')
        else:
            srcFileT1 = srcFileT1_lst[0]


    # define input output folder for this container
    dstDir_input = os.path.join(
        Dir_analysis,
        "sub-" + sub,
        "ses-" + ses,
        "input",
    )
    dstDir_output = os.path.join(
        Dir_analysis,
        "sub-" + sub,
        "ses-" + ses,
        "output",
    )
    
    # create corresponding folder
    if not os.path.exists(dstDir_input):
        os.makedirs(dstDir_input)
    if not os.path.exists(dstDir_output):
        os.makedirs(dstDir_output)
    
    if pre_fs:
        if not os.path.exists(os.path.join(dstDir_input, "pre_fs")):
            os.makedirs(os.path.join(dstDir_input, "pre_fs"))
    else:
        if not os.path.exists(os.path.join(dstDir_input, "anat")):
            os.makedirs(os.path.join(dstDir_input, "anat"))
    
    if annotfile:
        if os.path.isfile(annotfile):
            logger.info("\n"
                       +"Passed " + annotfile + ", copying to " + Dir_analysis)
            srcFileAnnot = os.path.join(Dir_analysis, "annotfile.zip")
            if os.path.isfile(srcFileAnnot):
                logger.info("\n"
                           +srcFileAnnot + " exists, if you want it new, delete it first")
            else:
                do.copy_file(annotfile, os.path.join(Dir_analysis, "annotfile.zip"), force)
        else:
            logger.info("\n"
                       +annotfile + " does not exist")
        if not os.path.exists(os.path.join(dstDir_input, "annotfile")):
            os.makedirs(os.path.join(dstDir_input, "annotfile"))
    # seems not implemented
    if mniroizip:
        if os.path.isfile(mniroizip):
            logger.info("\n"
                       +"Passed " + mniroizip + ", copying to " + Dir_analysis)
            srcFileMiniroizip = os.path.join(Dir_analysis, "mniroizip.zip")
            if os.path.isfile(srcFileMiniroizip):
                logger.warning("\n"+
                           srcFileMiniroizip + " exists, if you want it new, delete it first")
            else:
                do.copy_file(mniroizip, os.path.join(Dir_analysis, "mniroizip.zip"), force)
        else:
            logger.warning("\n"
                       +mniroizip + " does not exist")
        if not os.path.exists(os.path.join(dstDir_input, "mniroizip")):
            os.makedirs(os.path.join(dstDir_input, "mniroizip"))

    # Create the target files
    if pre_fs:
        dstFileT1 = os.path.join(dstDir_input, "pre_fs", "existingFS.zip")
    else:
        dstFileT1 = os.path.join(dstDir_input, "anat", "T1.nii.gz")

    dstFileAnnot = os.path.join(dstDir_input, "annotfile", "annots.zip")
    dstFileMniroizip = os.path.join(dstDir_input, "mniroizip", "mniroizip.zip")

    # Create the symbolic links
    
    force_symlink(srcFileT1, dstFileT1, force)
    logger.info("\n"
               +"-----------------The symlink created-----------------------\n")
    if annotfile:
        force_symlink(srcFileAnnot, dstFileAnnot, force)
    if mniroizip:
        force_symlink(srcFileMiniroizip, dstFileMniroizip, force)
   
    return 
   

#%%
def rtppreproc(parser_namespace, Dir_analysis, lc_config, sub, ses, layout):
    """
    Parameters
    ----------
    parser_namespace: parser obj
        it contains all the input argument in the parser

    lc_config : dict
        the lc_config dictionary from _read_config
    sub : str
        the subject name looping from df_subSes
    ses : str
        the session name looping from df_subSes.
    
    Returns
    -------
    none, create symbolic links

    """

    # define local variables from lc_config dict
    # input from get_parser
    container_specific_config_path= parser_namespace.container_specific_config
    run_lc=parser_namespace.run_lc
    
    # general level variables:
    
    basedir = lc_config["general"]["basedir"]
    container = lc_config["general"]["container"]
    bidsdir_name=lc_config["general"]["bidsdir_name"]  
    force = (lc_config["general"]["force"])
    # if force is False, then we don't want to overwrite anything
    # if force is true, and we didn't run_lc(in the prepare mode), we will do the overwrite and so on
    # if force is true and we do run_lc, then we will never overwrite
    force=force and (not run_lc)
    # container specific:
    precontainer_anat = lc_config["container_specific"][container]["precontainer_anat"]
    anat_analysis_name = lc_config["container_specific"][container]["anat_analysis_name"]
    rpe = lc_config["container_specific"][container]["rpe"]
    version = lc_config["container_specific"][container]["version"]
    
    srcFile_container_config_json= container_specific_config_path[0]
    

    container_specific_config_data = json.load(open(srcFile_container_config_json))
    # if version =='1.2.0-3.0.3':
    #     phaseEnco_direc = container_specific_config_data["config"]["pe_dir"]
    # if version =='1.1.3':
    #     phaseEnco_direc = container_specific_config_data["config"]["acqd"]
    
    phaseEnco_direc = container_specific_config_data["config"]["acqd"]
    # the source directory that stores the output of previous anatrois analysis
    srcDirFs = os.path.join(
        basedir,
        bidsdir_name,
        "derivatives",
        f'{precontainer_anat}',
        "analysis-" + anat_analysis_name,
        "sub-" + sub,
        "ses-" + ses,
        "output",
    )

    # define the source file, this is where symlink will point to
    # T1 file in anatrois output
    srcFileT1 = os.path.join(srcDirFs, "T1.nii.gz")
    # brain mask file in anatrois output
    logger.debug(f'\n the precontainer_ana is {precontainer_anat}')
    if int(precontainer_anat.split('.')[1])<6: 
        srcFileMask = os.path.join(srcDirFs, "brainmask.nii.gz")
    if int(precontainer_anat.split('.')[1])>5: 
        srcFileMask = os.path.join(srcDirFs, "brain.nii.gz")
    
    # 3 dwi file that needs to be preprocessed, under BIDS/sub/ses/dwi
    # the nii
    srcFileDwi_nii = layout.get(subject= sub, session=ses, extension='nii.gz',suffix= 'dwi',return_type='filename')[0]
    # the bval
    srcFileDwi_bval = layout.get(subject= sub, session=ses, extension='bval',suffix= 'dwi',return_type='filename')[0]
    # the bve
    srcFileDwi_bvec =layout.get(subject= sub, session=ses, extension='bvec',suffix= 'dwi',return_type='filename')[0]
    
    # check how many *dir_dwi.nii.gz there are in the BIDS/sub/ses/dwi directory
    phaseEnco_direc_dwi_files = layout.get(subject= sub, session=ses, extension='nii.gz',suffix= 'dwi', direction=phaseEnco_direc, return_type='filename')[0]
    
    if len(phaseEnco_direc_dwi_files) > 1:
        dwi_acq = [f for f in phaseEnco_direc_dwi_files if 'acq-' in f]
        if len(dwi_acq) == 0:
            logger.warning("\n"
                       +f"No files with different acq- to concatenate.\n")
        elif len(dwi_acq) == 1:
            logger.warning("\n"
                       +f"Found only {dwi_acq[0]} to concatenate. There must be at least two files with different acq.\n")
        elif len(dwi_acq) > 1:
            if not os.path.isfile(srcFileDwi_nii):
                logger.info("\n"
                           +f"Concatenating with mrcat of mrtrix3 these files: {dwi_acq} in: {srcFileDwi_nii} \n")
                dwi_acq.sort()
                sp.run(['mrcat',*dwi_acq,srcFileDwi_nii])
            # also get the bvecs and bvals
            bvals_dir = layout.get(subject= sub, session=ses, extension='bval',suffix= 'dwi', direction=phaseEnco_direc, return_type='filename')[0]
            bvecs_dir = layout.get(subject= sub, session=ses, extension='bvec',suffix= 'dwi', direction=phaseEnco_direc, return_type='filename')[0]
            bvals_acq = [f for f in bvals_dir if 'acq-' in f]
            bvecs_acq = [f for f in bvecs_dir if 'acq-' in f]
            if len(dwi_acq) == len(bvals_acq) and not os.path.isfile(srcFileDwi_bval):
                bvals_acq.sort()
                bval_cmd = "paste -d ' '"
                for bvalF in bvals_acq:
                    bval_cmd = bval_cmd+" "+bvalF
                bval_cmd = bval_cmd+" > "+srcFileDwi_bval
                sp.run(bval_cmd,shell=True)
            else:
                logger.warning("\n"
                           +"Missing bval files")
            if len(dwi_acq) == len(bvecs_acq) and not os.path.isfile(srcFileDwi_bvec):
                bvecs_acq.sort()
                bvec_cmd = "paste -d ' '"
                for bvecF in bvecs_acq:
                    bvec_cmd = bvec_cmd+" "+bvecF
                bvec_cmd = bvec_cmd+" > "+srcFileDwi_bvec
                sp.run(bvec_cmd,shell=True)
            else:
                logger.warning("\n"
                           +"Missing bvec files")
    # check_create_bvec_bval（force) one of the todo here
    if rpe:
        if phaseEnco_direc == "PA":
            rpe_dir = "AP"
        elif phaseEnco_direc == "AP":
            rpe_dir = "PA"
        # the reverse direction nii.gz
        srcFileDwi_nii_R = layout.get(subject= sub, session=ses, extension='nii.gz',suffix= 'dwi', direction=rpe_dir, return_type='filename')[0]
        # the reverse direction bval
        srcFileDwi_bval_R = layout.get(subject= sub, session=ses, extension='bval',suffix= 'dwi', direction=rpe_dir, return_type='filename')[0]
        # the reverse direction bvec
        srcFileDwi_bvec_R =layout.get(subject= sub, session=ses, extension='bvec',suffix= 'dwi', direction=rpe_dir, return_type='filename')[0]

        # If bval and bvec do not exist because it is only b0-s, create them
        # (it would be better if dcm2niix would output them but...)
        # build the img matrix according to the shape of nii.gz
        img = nib.load(srcFileDwi_nii_R)
        volumes = img.shape[3]
        # if one of the bvec and bval are not there, re-write them
        if (not os.path.isfile(srcFileDwi_bval_R)) or (not os.path.isfile(srcFileDwi_bvec_R)):
            # Write bval file
            f = open(srcFileDwi_bval_R, "x")
            f.write(volumes * "0 ")
            f.close()

            # Write bvec file
            f = open(srcFileDwi_bvec_R, "x")
            f.write(volumes * "0 ")
            f.write("\n")
            f.write(volumes * "0 ")
            f.write("\n")
            f.write(volumes * "0 ")
            f.write("\n")
            f.close()

    # create input and output directory for this container, the dstDir_output should be empty, the dstDir_input should contains all the symlinks
    dstDir_input = os.path.join(
        Dir_analysis,
        "sub-" + sub,
        "ses-" + ses,
        "input",
    )
    dstDir_output = os.path.join(
        Dir_analysis,
        "sub-" + sub,
        "ses-" + ses,
        "output",
    )

    if not os.path.exists(dstDir_input):
        os.makedirs(dstDir_input)
    if not os.path.exists(dstDir_output):
        os.makedirs(dstDir_output)
    # destination directory under dstDir_input
    if not os.path.exists(os.path.join(dstDir_input, "ANAT")):
        os.makedirs(os.path.join(dstDir_input, "ANAT"))
    if not os.path.exists(os.path.join(dstDir_input, "FSMASK")):
        os.makedirs(os.path.join(dstDir_input, "FSMASK"))
    if not os.path.exists(os.path.join(dstDir_input, "DIFF")):
        os.makedirs(os.path.join(dstDir_input, "DIFF"))
    if not os.path.exists(os.path.join(dstDir_input, "BVAL")):
        os.makedirs(os.path.join(dstDir_input, "BVAL"))
    if not os.path.exists(os.path.join(dstDir_input, "BVEC")):
        os.makedirs(os.path.join(dstDir_input, "BVEC"))
    if rpe:
        if not os.path.exists(os.path.join(dstDir_input, "RDIF")):
            os.makedirs(os.path.join(dstDir_input, "RDIF"))
        if not os.path.exists(os.path.join(dstDir_input, "RBVL")):
            os.makedirs(os.path.join(dstDir_input, "RBVL"))
        if not os.path.exists(os.path.join(dstDir_input, "RBVC")):
            os.makedirs(os.path.join(dstDir_input, "RBVC"))

    # Create the destination paths
    dstT1file = os.path.join(dstDir_input, "ANAT", "T1.nii.gz")
    dstMaskFile = os.path.join(dstDir_input, "FSMASK", "brainmask.nii.gz")

    dstFileDwi_nii = os.path.join(dstDir_input, "DIFF", "dwiF.nii.gz")
    dstFileDwi_bval = os.path.join(dstDir_input, "BVAL", "dwiF.bval")
    dstFileDwi_bvec = os.path.join(dstDir_input, "BVEC", "dwiF.bvec")

    if rpe:
        dstFileDwi_nii_R = os.path.join(dstDir_input, "RDIF", "dwiR.nii.gz")
        dstFileDwi_bval_R = os.path.join(dstDir_input, "RBVL", "dwiR.bval")
        dstFileDwi_bvec_R = os.path.join(dstDir_input, "RBVC", "dwiR.bvec")

    
    
    # Create the symbolic links
    force_symlink(srcFileT1, dstT1file, force)
    force_symlink(srcFileMask, dstMaskFile, force)
    force_symlink(srcFileDwi_nii, dstFileDwi_nii, force)
    force_symlink(srcFileDwi_bval, dstFileDwi_bval, force)
    force_symlink(srcFileDwi_bvec, dstFileDwi_bvec, force)
    logger.info("\n"
               +"-----------------The rtppreproc symlinks created\n")
    if rpe:
        force_symlink(srcFileDwi_nii_R, dstFileDwi_nii_R, force)
        force_symlink(srcFileDwi_bval_R, dstFileDwi_bval_R, force)
        force_symlink(srcFileDwi_bvec_R, dstFileDwi_bvec_R, force)
        logger.info("\n"
                   +"---------------The rtppreproc rpe=True symlinks created")
    return 



#%%
def rtppipeline(parser_namespace, Dir_analysis,lc_config,sub, ses, layout):
    """
    Parameters
    ----------
    lc_config : dict
        the lc_config dictionary from _read_config
    sub : str
        the subject name looping from df_subSes
    ses : str
        the session name looping from df_subSes.
    container_specific_config_path : str
        
    Returns
    -------
    none, create symbolic links

    """
    # define local variables from config dict
    # input from get_parser


    run_lc=parser_namespace.run_lc
    
    # general level variables:
    basedir = lc_config["general"]["basedir"]
    container = lc_config["general"]["container"]
    bidsdir_name=lc_config["general"]["bidsdir_name"]  
    force = (lc_config["general"]["force"])
    force = force and (not run_lc)
    # rtppipeline specefic variables

    precontainer_anat = lc_config["container_specific"][container]["precontainer_anat"]
    anat_analysis_name = lc_config["container_specific"][container]["anat_analysis_name"]
    precontainer_preproc = lc_config["container_specific"][container]["precontainer_preproc"]
    preproc_analysis_num = lc_config["container_specific"][container]["preproc_analysis_name"]
    # there is a bug before, when create symlinks the full path of trachparams are not passed, very weired
    srcFile_tractparams= os.path.join(Dir_analysis, "tractparams.csv")

    # the source directory
    srcDirfs = os.path.join(
        basedir,
        bidsdir_name,
        "derivatives",
                    f'{precontainer_anat}',
        "analysis-" + anat_analysis_name,
        "sub-" + sub,
        "ses-" + ses,
        "output",
    )
    srcDirpp = os.path.join(
        basedir,
        bidsdir_name,
        "derivatives",
        precontainer_preproc,
        "analysis-" + preproc_analysis_num,
        "sub-" + sub,
        "ses-" + ses,
        "output",
    )
    # the source file
    srcFileT1 = os.path.join(srcDirpp, "t1.nii.gz")
    srcFileFs = os.path.join(srcDirfs, "fs.zip")
    srcFileDwi_bvals = os.path.join(srcDirpp, "dwi.bvals")
    srcFileDwi_bvec = os.path.join(srcDirpp, "dwi.bvecs")
    srcFileDwi_nii = os.path.join(srcDirpp, "dwi.nii.gz")

    # creat input and output directory for this container, the dstDir_output should be empty, the dstDir_input should contains all the symlinks
    dstDir_input = os.path.join(
        Dir_analysis,
        "sub-" + sub,
        "ses-" + ses,
        "input",
    )
    dstDir_output = os.path.join(
        Dir_analysis,
        "sub-" + sub,
        "ses-" + ses,
        "output",
    )

    # under dstDir_input there are a lot of dir also needs to be there to have symlinks
    if not os.path.exists(dstDir_input):
        os.makedirs(dstDir_input)
    if not os.path.exists(dstDir_output):
        os.makedirs(dstDir_output)
    if not os.path.exists(os.path.join(dstDir_input, "anatomical")):
        os.makedirs(os.path.join(dstDir_input, "anatomical"))
    if not os.path.exists(os.path.join(dstDir_input, "fs")):
        os.makedirs(os.path.join(dstDir_input, "fs"))
    if not os.path.exists(os.path.join(dstDir_input, "dwi")):
        os.makedirs(os.path.join(dstDir_input, "dwi"))
    if not os.path.exists(os.path.join(dstDir_input, "bval")):
        os.makedirs(os.path.join(dstDir_input, "bval"))
    if not os.path.exists(os.path.join(dstDir_input, "bvec")):
        os.makedirs(os.path.join(dstDir_input, "bvec"))
    if not os.path.exists(os.path.join(dstDir_input, "tractparams")):
        os.makedirs(os.path.join(dstDir_input, "tractparams"))

    # Create the destination file
    dstAnatomicalFile = os.path.join(dstDir_input, "anatomical", "T1.nii.gz")
    dstFsfile = os.path.join(dstDir_input, "fs", "fs.zip")
    dstDwi_niiFile = os.path.join(dstDir_input, "dwi", "dwi.nii.gz")
    dstDwi_bvalFile = os.path.join(dstDir_input, "bval", "dwi.bval")
    dstDwi_bvecFile = os.path.join(dstDir_input, "bvec", "dwi.bvec")
    dst_tractparams = os.path.join(dstDir_input, "tractparams", "tractparams.csv")

    dstFile_tractparams = os.path.join(Dir_analysis, "tractparams.csv")

    # the tractparams check, at the analysis folder 
    tractparam_df =read_df(dstFile_tractparams)
    check_tractparam(lc_config, sub, ses, tractparam_df)


    # Create the symbolic links
    force_symlink(srcFileT1, dstAnatomicalFile, force)
    force_symlink(srcFileFs, dstFsfile, force)
    force_symlink(srcFileDwi_nii, dstDwi_niiFile, force)
    force_symlink(srcFileDwi_bvec, dstDwi_bvecFile, force)
    force_symlink(srcFileDwi_bvals, dstDwi_bvalFile, force)
    force_symlink(srcFile_tractparams, dst_tractparams, force)
    logger.info("\n"
               +"-----------------The rtppipeline symlinks created\n")
    return 
    
