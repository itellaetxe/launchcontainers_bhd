clear all
% VIENNA
% addpath(genpath('/ceph/mri.meduniwien.ac.at/departments/physics/fmrilab/lab/presurfer'))
% addpath(genpath('/ceph/mri.meduniwien.ac.at/departments/physics/fmrilab/lab/NORDIC_Raw'))
% baseP = '/ceph/mri.meduniwien.ac.at/projects/physics/fmri/data/bcblvie22/BIDS';

% BCBL


%softPath = fileparts(bvRootPath);
baseP = fullfile('/bcbl/home/public/Gari/SENSOTIVE');
addpath(genpath(fullfile(baseP,'soft','presurfer')));
addpath(genpath(fullfile(baseP,'soft','NORDIC_Raw')));
addpath(genpath(fullfile(baseP,'soft','spm12')));
baseP = fullfile(baseP,'BIDS');

%baseP = fullfile('/bcbl/home/public/Gari/SENSOTIVE/NORDIC_PILOT','Nifti');

setenv('FSLOUTPUTTYPE', 'NIFTI_GZ')
%subs = {'GariTest','DavidTest'}; %{'bt001','bt002'};

subs = {'p002'};
% subs = {'DavidTest'}; %{'bt001','bt002'};
sess = {'002'};%'001',

doPresurfer = true;
doNORDIC = true;
dotsnr = true;

for subI=1:length(subs)
    sub = ['sub-',subs{subI}];
    parfor sesI=1:length(sess)
        ses = ['ses-',sess{sesI}];
        sesP = fullfile(baseP, sub, ses);
        
        %% first run presurfer to denoise mp2rage images
        T1w_out = fullfile(sesP, 'anat', [sub,'_',ses,'_T1w.nii']);
        if ~exist([T1w_out,'.gz'], 'file') && doPresurfer
            % define the files
            UNI  = fullfile(sesP, 'anat', [sub,'_',ses,'_T1_uni.nii']);
            INV2 = fullfile(sesP, 'anat', [sub,'_',ses,'_T1_inv2.nii']);
            try
                % unzip the data
                gunzip([UNI,'.gz']);
                gunzip([INV2,'.gz']);
                
                % STEP - 0 : (optional) MPRAGEise UNI
                UNI_out = presurf_MPRAGEise(INV2,UNI);
                
                % move, rename, clean up
                system(['cp ', UNI_out, ' ', T1w_out]);
                pause(2);
                gzip(T1w_out);
                system(['rm ', T1w_out, ' ', UNI, ' ', INV2]);
                system(['rm -r ', fullfile(sesP, 'anat', 'presurf_MPRAGEise')]);
            end
        end
    end
end

for subI=1:length(subs)
    sub = ['sub-',subs{subI}];
    for sesI=1:length(sess)
        ses = ['ses-',sess{sesI}];
        sesP = fullfile(baseP, sub, ses);
        
        %% perform nordic on all the funtional files
        mags = dir(fullfile(sesP, 'func', '*_magnitude.nii.gz'));
        
        parfor magI=1:length(mags)
            try
                % define file names
                fn_magn_in  = fullfile(mags(magI).folder, mags(magI).name);
                fn_phase_in = strrep(fn_magn_in, 'magnitude', 'phase');
                
                if ~exist(strrep(fn_magn_in, '.nii.gz', '_orig.nii.gz'), 'file') && doNORDIC
                    
                    info = niftiinfo(fn_magn_in);
                    system(['cp ', fn_magn_in, ' ', strrep(fn_magn_in, '.nii.gz', '_orig.nii.gz')]);
                    system(['cp ', fn_phase_in, ' ', strrep(fn_phase_in, '.nii.gz', '_orig.nii.gz')]);
                    system(['chmod 755 ', fn_phase_in, ' ', fn_magn_in]);
                    system(['fslroi ', fn_magn_in, ' ', fn_magn_in, ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end)-4)]);
                    system(['fslroi ', fn_phase_in, ' ', fn_phase_in, ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end)-4)]);
                    system(['fslmaths ', fn_magn_in,  ' ', fn_magn_in,  ' -odt float']);
                    system(['fslmaths ', fn_phase_in, ' ', fn_phase_in, ' -odt float']);
                end
            end
        end
        
        %% perform nordic on all the funtional files
        I = 1;
        for magI=1:length(mags)
            % define file names
            fn_magn_in  = fullfile(mags(magI).folder, mags(magI).name);
            fn_phase_in = strrep(fn_magn_in, 'magnitude', 'phase');
            fn_out      = strrep(fn_magn_in, 'magnitude', 'bold');
            
            if ~(exist(strrep(fn_out, '.nii.gz', 'magn.nii'), 'file') || exist(fn_out,'file')) && doNORDIC
                
                ARG(I).temporal_phase = 1;
                ARG(I).phase_filter_width = 10;
                ARG(I).noise_volume_last = 1;
                [ARG(I).DIROUT,fn_out_name,~] = fileparts(fn_out);
                ARG(I).DIROUT = [ARG(I).DIROUT, '/'];
                ARG(I).make_complex_nii = 1;
                ARG(I).save_gfactor_map = 1;
                
                file(I).phase = fn_phase_in;
                file(I).magni = fn_magn_in;
                file(I).out   = strrep(fn_out_name, '.nii', '');
                
                I = I + 1;
            end
        end
        
        if exist('ARG', 'var')
            parfor i=1:length(ARG)
                %              try
                NIFTI_NORDIC(file(i).magni, file(i).phase,file(i).out,ARG(i));
                %              end
            end
            clear ARG file
        end
        
        for magI=1:length(mags)
            %             try
            % define file names
            fn_magn_in  = fullfile(mags(magI).folder, mags(magI).name);
            fn_phase_in = strrep(fn_magn_in, 'magnitude', 'phase');
            fn_out      = strrep(fn_magn_in, 'magnitude', 'bold');
            gfactorFile = strrep(strrep(fn_out, '.nii.gz', '.nii'),[sub '_ses'],['gfactor_' sub '_ses']);
            
            if exist(gfactorFile, 'file') && doNORDIC
                % clean up
                info = niftiinfo(fn_magn_in);
                system(['fslroi ', strrep(fn_out, '.nii.gz', 'magn.nii'), ' ', fn_out, ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end)-1)]);
                gzip(gfactorFile);
                system(['rm ', strrep(fn_out, '.nii.gz', 'magn.nii'), ' ', gfactorFile]);
                system(['mv ', strrep(gfactorFile, '.nii', '.nii.gz'), ' ', strrep(strrep(strrep(gfactorFile, '.nii', '.nii.gz'), '_bold', '_gfactor'), 'gfactor_', '')]);
            end
      
            % copy the events.tsv
            if ~exist(strrep(fn_magn_in, 'magnitude.nii.gz', 'bold.json'), 'file')
                system(['cp ', strrep(fn_magn_in, 'magnitude.nii.gz', 'magnitude.json'), ' ', ...
                    strrep(fn_magn_in, 'magnitude.nii.gz', 'bold.json')]);
            end
            if ~doNORDIC
                info = niftiinfo(fn_magn_in);
                system(['cp ',fn_magn_in, ' ',  strrep(fn_magn_in, '_magnitude', '_bold')]);
                system(['chmod 755 ', strrep(fn_magn_in, '_magnitude', '_bold')]);
                system(['fslroi ', strrep(fn_magn_in, '_magnitude', '_bold'), ' ', ...
                    strrep(fn_magn_in, '_magnitude', '_bold'), ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end)-5)]);
            end
            %             end
            
            % rename sbref
            sbref_mags = dir(fullfile(sesP, 'func', '*_part-mag_sbref.nii.gz'));
            if ~isempty(sbref_mags)
                for sbref_magI = 1:length(sbref_mags)
                    sbref_mag = fullfile(sbref_mags(sbref_magI).folder, sbref_mags(sbref_magI).name);
                    if ~exist(strrep(sbref_mag, '_part-mag_sbref.nii.gz', '_sbref.json'), 'file')
                        system(['cp ', sbref_mag, ' ', strrep(sbref_mag, '_part-mag', '')]);
                        system(['cp ', strrep(sbref_mag, '.nii.gz', '.json'), ' ', ...
                            strrep(sbref_mag, '_part-mag_sbref.nii.gz', '_sbref.json')]);
                    end
                end
            end
            
            
            
            if dotsnr
                bolds = dir(fullfile(sesP, 'func', '*bold.nii.gz'));
                mags  = dir(fullfile(sesP, 'func', '*magnitude.nii.gz'));
                bolds(contains({bolds.name}, 'gfactor')) = [];
                
                parfor nb=1:length(bolds)
                    fprintf("\n\n%s_%s_run-0%i",sub,ses,nb)
                    % Define file names
                    magFile  = fullfile(mags(nb).folder, mags(nb).name);
                    boldFile = fullfile(bolds(nb).folder, bolds(nb).name);
                    
                    
                    tsnrFile = strrep(boldFile,'bold','tsnr_postNordic');
                    magtsnrFile = strrep(boldFile,'bold','tsnr_preNordic');
                    gfactorFile = strrep(boldFile,'bold','gfactor');
                    tsnrGfactorFile = strrep(gfactorFile,'gfactor','gfactorSameSpace');
                    
                    % pre NORDIC tSNR
                    magData = single(niftiread(magFile));
                    magHeader = niftiinfo(magFile);
                    magtsnrData = mean(magData,4) ./ std(magData,1,4);
                    magHeader.ImageSize = size(magtsnrData);
                    magHeader.PixelDimensions=magHeader.PixelDimensions(1:3);
                    niftiwrite(magtsnrData, strrep(magtsnrFile, '.nii', ''), magHeader,'compressed',true)
                    
                    % post NORDIC tSNR
                    boldData = niftiread(boldFile);
                    boldHeader = niftiinfo(boldFile);
                    tsnrData = mean(boldData,4) ./ std(boldData,1,4);
                    boldHeader.ImageSize = size(tsnrData);
                    boldHeader.PixelDimensions=boldHeader.PixelDimensions(1:3);
                    niftiwrite(tsnrData, strrep(tsnrFile, '.nii', ''),boldHeader,'compressed',true)
                    
                    % Write g factor in same space
                    gfactorData = niftiread(gfactorFile);
                    gHeader = magHeader;
                    gHeader.ImageSize=size(gfactorData);
                    gHeader.PixelDimensions=gHeader.PixelDimensions(1:3);
                    niftiwrite(gfactorData, strrep(tsnrGfactorFile, '.nii', ''), gHeader,'compressed',true)
                    
                    
                end
            end
        end
    end
end
