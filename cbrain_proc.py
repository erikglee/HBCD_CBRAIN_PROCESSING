import requests, json, getpass, os
from datetime import date
import numpy as np
import boto3
from botocore.exceptions import ClientError
import pathlib
from pathlib import Path
import glob
import logging
import inspect
import pandas as pd
import botocore
import datetime 
import re
import base64
from io import BytesIO
import matplotlib.pyplot as plt



#WHAT DO WE NEED TO IMPLEMENT.
#1. Step 1 - find a list of all subjects registered in CBRAIN
#2. Step 2 - find which pipelines have been run for each subject
#3. Step 3 - find which subjects have the prerequisite files for CBRAIN processing.
#4. Step 4 - (optional) find which subjects are currently processing with different pipelines and which subjects have previously failed processing in different pipelines
#5  Step 5 - based on results of steps 1-4, find which subjects need to be processed in the current pipeline
#6  Step 6 - find files that should be included in specific processing pipeline for desired subjects
#7  Step 7 - make a CBRAIN CSV file

#Comment - can LORIS make a BIDS file exclusion list that I can reference?
#In what cases are we fine with running processing with incomplete data?
#Only T1 or only T2?
#One fMRI scan + field maps
#One DWI scan + field masp
#Partial EEG acquisition?

def find_cbrain_subjects(cbrain_api_token, data_provider_id = 710): #For the real study this should be 710
    '''Find a list of subjects registered in CBRAIN

    Parameters
    ----------
    cbrain_api_token : str
        The api token generated when you logged into cbrain
    data_provider_id : int, default 710
        The CBRAIN data provider ID for the location where
        where all your BIDS data is stored
    
    Returns
    -------
    cbrain_subjects : list
        List of BidsSubjects in CBRAIN that
        belong to the specified dataprovider
    ids : list
        List of CBRAIN file IDs for each of
        the BidsSubjects in the same ordering
        as cbrain_subjects
    sizes : list
        List of CBRAIN file sizes for each of
        the BidsSubjects in the same ordering
        as cbrain_subjects
    
    '''

    files = []
    files_request = {
        'cbrain_api_token': cbrain_api_token,
        'page': 1,
        'per_page': 1000
    }

    while True:

        base_url = 'https://portal.cbrain.mcgill.ca'
        files_response = requests.get(
            url = '/'.join([base_url, 'userfiles']),
            data = files_request,
            headers = {'Accept': 'application/json'}
        )

        if files_response.status_code != requests.codes.ok:
            print('User files request failed.')
            print(files_response)
            break

        # Collect the responses on this page then increment
        files += files_response.json()
        files_request['page'] += 1

        # Stop requesting responses when we're at the last page
        if len(files_response.json()) < files_request['per_page']:
            break 

    file_names = []
    ids = []
    sizes = []
    data_provider_id = int(data_provider_id)
    for temp in files:
        if (temp['data_provider_id'] == data_provider_id) and (temp['type'] == 'BidsSubject'):
            if temp['name'] not in file_names:
                file_names.append(temp['name'])
                ids.append(temp['id'])
                sizes.append(temp['size'])
        
    return file_names, ids, sizes

def find_s3_subjects(bids_bucket_config, bucket = 'hbcd-pilot', prefix = 'assembly_bids'):
    '''Utility to find BIDS subjects in S3 bucket
    
    Parameters
    ----------
    
    bids_bucket_config : str
        This will be used as a config file to identify
        the s3 credentials
    bucket : str, default 'hbcd-pilot'
        The name of the bucket to query for subjects
    prefix : str, default 'assembly_bids'
        The prefix to restrict the files returned by
        the search query (i.e. if data is at
        s3://hbcd-pilot/assembly_bids/sub-1, then
        prefix = 'assembly_bids')
        
    Returns
    -------
    
    s3_subjects : list
        List of "subjects", meaning any instance where
        the path of a folder in the s3 bucket had 'sub-'
        afte the first '/' following the prefix.
        This doesn't mean that the file actually
        corresponds to a BIDS subject, just that it satisfies
        this simple naming pattern.
        
    '''

    # Create a PageIterator    
    page_iterator = create_page_iterator(bucket = bucket, prefix = prefix, bucket_config = bids_bucket_config)

    #Iterate through bucket to find potential subjects
    s3_contents = []
    potential_subjects = []
    #potential_subjects_dates = []
    for page in page_iterator:
        if page.get('Contents', None):
            for temp_dict in page['Contents']:
                potential_subjects.append(temp_dict['Key'].split('/')[1])
                #potential_subjects_dates.append(temp_dict['LastModified'])

    #Find unique files starting with "sub-*"
    potential_subjects = list(set(potential_subjects))
    s3_subjects = []
    for temp_file in potential_subjects:
        if 'sub-' in temp_file:
            s3_subjects.append(temp_file)
            
    #return list of s3 subjects
    return s3_subjects


def create_page_iterator(bucket = 'hbcd-pilot', prefix = 'derivatives', bucket_config = False, return_client_instead = False):
    '''Utility to create a page iterator for s3 bucket'''
    
    #Grab config path
    if bucket_config == False:
        config_path = ''
    else:
        if type(bucket_config) != str:
            raise NameError('Error: different config path should eithe be string or boolean')
        else:
            config_path = bucket_config
            
    #Find info from config file    
    with open(config_path, 'r') as f:
        lines = f.read().splitlines()
        for temp_line in lines:
            if 'access_key' == temp_line[:10]:
                access_key = temp_line.split('=')[-1].strip()
            if 'secret_key' == temp_line[:10]:
                secret_key = temp_line.split('=')[-1].strip()
            if 'host_base' == temp_line[:9]:
                host_base = temp_line.split('=')[-1].strip()
                if 'https' != host_base[:5]:
                    host_base = 'https://' + host_base
        
    #Create s3 client
    client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url =host_base
    )

    del access_key, secret_key, host_base
    
    if return_client_instead:
        return client
        
    # Create a reusable Paginator
    paginator = client.get_paginator('list_objects')
        
    # Create a PageIterator from the Paginator
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    return page_iterator


def upload_processing_config_log(file_name, bucket = 'hbcd-cbrain-test', prefix = 'cbrain_misc/cbrain_processing_configuration_logs', bucket_config = False):
    """Upload a CBRAIN CSV File to S3 Bucket

    This function will upload an already generated
    CBRAIN CSV File (i.e. extended file list) to
    the desired bucket. Then it can later be registered
    in CBRAIN and used to run processing.

    Parameters
    ----------
    file_name : str
        The name of the file to upload
    bucket : str, default 'hbcd-cbrain-test'
        The name of the bucket to upload to
    prefix : str, default 'cbrain_misc/cbrain_csvs'
        The prefix to use for the file in the bucket
    bids_bucket_config : str, default False
        The path to the config file for the s3 bucket
        If False, then the default config file will be used.
        If a string, then that string will be used as the
        path to the config file.

    Returns
    -------
    bool
        True if file was uploaded successfully,
        False if not.
    """

    
    #Grab config path
    if bucket_config == False:
        config_path = ''
    else:
        if type(bucket_config) != str:
            raise NameError('Error: different config path should eithe be string or boolean')
        else:
            config_path = bucket_config
            
    #Find info from config file    
    with open(config_path, 'r') as f:
        lines = f.read().splitlines()
        for temp_line in lines:
            if 'access_key' == temp_line[:10]:
                access_key = temp_line.split('=')[-1].strip()
            if 'secret_key' == temp_line[:10]:
                secret_key = temp_line.split('=')[-1].strip()
            if 'host_base' == temp_line[:9]:
                host_base = temp_line.split('=')[-1].strip()
                if 'https' != host_base[:5]:
                    host_base = 'https://' + host_base
        
    #Create s3 client
    client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url =host_base
    )

    del access_key, secret_key, host_base

    object_name = os.path.join(prefix, os.path.basename(file_name))

    # Upload the file
    try:
        response = client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def check_bids_requirements(subject_id, requirements_dict, qc_df = None, bucket = 'hbcd-pilot', prefix = 'assembly_bids',
                            bids_bucket_config = False, session = None, session_agnostic_files = ['sessions.tsv']):
    '''Utility to check if BIDS subject has required files for procesing.

    Example
    -------

    For each parent requirement (FMRI, T1, T2), check that at least one file
    exists that either does or doesn't have the specified content in it's name.
    So T1, there should be a file that has "T1w.nii.gz" in it's name, and not
    have "rec-undistorted" and "acq-svslocalizer" in it's name.
    requirements_dict = {
        "FMRI" : {"bold.nii.gz" : true, "rec-undistorted" : false, "rec-biascorrected" : false},
        "T1" : {"T1w.nii.gz" : true, "rec-undistorted" : false, "acq-svslocalizer" : false},
        "T2" : {"T2w.nii.gz" : true, "rec-undistorted" : false, "acq-svslocalizer" : false}
    }
    requirements_satisfied = ('sub-01', requirements_dict)

    The function will return True if FMRI, T1, and T2 requirements are ALL
    satisfied, and False if any of them are not.
    
    Parameters
    ----------
    
    subject_id : str
        Subject whose BIDS requirements are being checked
        (i.e. sub-01)
    requirements_dict : dict
        Dictionary of file requirements that must be satisfied
        for processing.
    bucket : str, default 'hbcd_cbrain_test'
        The name of the bucket to query for files
    prefix : str, default 'bids/sub'
        The prefix to restrict the files returned by
        the search query 
    bids_bucket_config : bool or str, default False
        If bids_bucket_config is a string, this
        will be used as a config file to identify
        the s3 credentials... otherwise the default
        location will be used
        
    Returns
    -------
    
    bool
        True if requirements are all satisfied,
        otherwise False
        
    '''

    # Create a dictionary that will store information about the
    # different processing requirements. This is not actually
    # used for processing, but will be used to build a spreadsheet
    # that describes which subjects were/werent processed.
    requirements_tracking_dict = {}
    for temp_req in requirements_dict.keys():
        requirements_tracking_dict[temp_req] = 'No File'

    
    # Do some cosmetics so that the prefix includes the pipeline
    # name and so that later we look for the subject ID at the
    # right level
    full_prefix = os.path.join(prefix, subject_id)
    prefix_offset = len(full_prefix.split('/')) - 1
    if len(prefix) == 0:
        prefix_offset = prefix_offset - 1
        
    # Create a PageIterator from the Paginator
    page_iterator = create_page_iterator(bucket = bucket, prefix = full_prefix, bucket_config = bids_bucket_config)

    #Iterate through bucket to find BIDS files for this subject
    subject_files = []
    for page in page_iterator:
        if 'Contents' in page:
            for temp_dict in page['Contents']:
                subject_files.append(temp_dict['Key'].split('/')[-1])
        else:
            print('No BIDS contents found for: {}'.format(subject_id))

    parent_requirements_satisfied = 0
    for parent_requirement in requirements_dict.keys():
        temp_tracking_status = 'No File'
        child_requirements = requirements_dict[parent_requirement]
        for temp_file in subject_files:
            child_requirements_satisfied = 0
            skip_file = 0
            temp_ses = temp_file.split('_')[1]
            #Skip the file if (1) we are focusing on session specific files, and
            #(2) the current file is from a different session and (3) if the file
            #is a session agnostic file. 1,2,3 must all be true to skip a file
            if type(session) != type(None):
                if session != temp_ses:
                    for temp_ses_agnostic in session_agnostic_files:
                        if temp_ses_agnostic not in temp_file:
                            skip_file = 1
            if skip_file:
                continue
            for child_requirement in child_requirements['file_naming'].keys():
                if (child_requirement in temp_file) == child_requirements['file_naming'][child_requirement]:
                    child_requirements_satisfied += 1
                else:
                    break
            if child_requirements_satisfied == len(child_requirements['file_naming'].keys()):
                
                #Now check if the qc_criteria for the parent requirement is satisfied.
                #This only needs to be checked if qc_criteria is listed for the current
                #requirement and if a qc dataframe is provided
                requirement_disqualified = 0
                #print(parent_requirement)
                #print('Parent Requirement: {}'.format(parent_requirement))
                if (type(qc_df) != type(None)) and ('qc_criteria' in requirements_dict[parent_requirement].keys()):
                    
                    #Grab partial df with just the current file's ratings
                    partial_df = qc_df[qc_df['filename'].str.contains(temp_file.split('/')[-1])]
                    if len(partial_df) == 0:
                        print('    Warning: Subject has QC file but missing entries, retry proc later ({})'.format(temp_file))
                        if temp_tracking_status == 'No File': #This wont overwrite the status when requirements are satisfied
                            temp_tracking_status = 'No QC'

                        #Should there be a dry run option here where we dont
                        #return if we are just running this function to create
                        #the tracking log
                        return None, requirements_tracking_dict
                    
                    #Iterate through each QC requirement (the requirements are
                    #stored as a list of dictionaries, each with one key/value pair)
                    for temp_qc_criteria in requirements_dict[parent_requirement]['qc_criteria']:
                        
                        for temp_key in temp_qc_criteria.keys():
                            #If the requirement is a boolean, make sure the
                            #QC value matches that of the requirement
                            if type(temp_qc_criteria[temp_key]) == bool:
                                if partial_df[temp_key].values[0] != temp_qc_criteria[temp_key]:
                                    
                                    #As backup, check if yes/no was used instead of boolean
                                    if temp_qc_criteria[temp_key]:
                                        temp_bool_str = 'YES'
                                    else:
                                        temp_bool_str = 'NO'
                                    #print(partial_df[temp_key].values[0])
                                    #print(type(partial_df[temp_key].values[0]))
                                    if type(partial_df[temp_key].values[0]) != str:
                                        temp_val = str(partial_df[temp_key].values[0]).upper()
                                    else:
                                        temp_val = partial_df[temp_key].values[0].upper()
                                    if temp_val != temp_bool_str:
                                        #print(partial_df[temp_key].values[0])
                                        #print(temp_qc_criteria[temp_key])
                                        #print('{}: {}'.format(temp_key, partial_df[temp_key].values[0]))
                                        if (temp_tracking_status == 'No File') or (temp_tracking_status == 'No QC'):
                                            temp_tracking_status = 'Failed QC'
                                        requirement_disqualified = 1
                                        
                            #Otherwise assume the requirement is a number, and
                            #make sure the observed value is less than the requirement
                            else:
                                if (partial_df[temp_key].values[0] > temp_qc_criteria[temp_key]) or np.isnan(partial_df[temp_key].values[0]):
                                    #print('{}: {}'.format(temp_key, partial_df[temp_key].values[0]))
                                    if (temp_tracking_status == 'No File') or (temp_tracking_status == 'No QC'):
                                        temp_tracking_status = 'Failed QC'
                                    requirement_disqualified = 1
                             
                #print('Requirement Disqualified Status: {}\n'.format(requirement_disqualified))
                if requirement_disqualified == 0:
                    parent_requirements_satisfied += 1
                    temp_tracking_status = 'Satisfied'
                requirements_tracking_dict[parent_requirement] = temp_tracking_status
                break
    
    #If all requirements have been satisfied at least once return true, else false
    if parent_requirements_satisfied == len(requirements_dict.keys()):
        #print(len(requirements_dict.keys()))
        return True, requirements_tracking_dict
    else:
        return False, requirements_tracking_dict
    
    
def grab_required_bids_files(subject_id, requirements_dict, qc_df = None, bucket = 'hbcd-pilot', prefix = 'assembly_bids', 
                             bids_bucket_config = False, session = None, session_agnostic_files = ['sessions.tsv'],
                             associated_files_dict = None):
    '''Utility to grab the names of BIDS files required for processing.
    
    This function assumes check_bids_requirements
    has already been ran and that the user is fine
    with the number of requirements satisfied in the
    requirements dict. The goal of this script is to
    return a list of files that satisfies the requirements
    dict. The returned list will only have (at most) the
    number of requirements specified by num_requirements_dict.
    For example, the user can ask for 1, 2, or 'all' files that
    fullfill a given requirement. In the case that there are
    0 files fullfilling a requirement, the function will only
    return the file paths specified by the other requirements.

    Example
    -------

    #Only select files that satisfy FMRI, T1, or T2 requirements
    requirements_dict = {
        "FMRI" : {"bold.nii.gz" : true, "rec-undistorted" : false, "rec-biascorrected" : false},
        "T1" : {"T1w.nii.gz" : true, "rec-undistorted" : false, "acq-svslocalizer" : false},
        "T2" : {"T2w.nii.gz" : true, "rec-undistorted" : false, "acq-svslocalizer" : false}
        }
    
    #Of the files that satisfy any of the given requirements,
    #keep the desired number of files that satisfy each requirement.
    #This can either be an integer, or 'all' if every file should be
    #kept. Note that the requirements_dict and num_requirements_dict
    #must have the same keys.
    num_requirements_dict = {"T1" : 1", "T2" : 1", "FMRI" : "all"}

    files_list = grab_required_bids_files('sub-01', requirements_dict, num_requirements_dict)
    
    Parameters
    ----------
    subject_id : str
        Subject whose BIDS requirements are being checked.
        (i.e. sub-01)
    requirements_dict : dict
        Dictionary listing possible requirements that must be
        satisfied for a file to be included in processing.
        (See example above)
    num_requirements_dict : dict
        Dictionary listing the number of files that will be
        kept for any given requirement. (See example above)
    bucket : str, default 'hbcd-pilot'
        The name of the bucket to query for files
    prefix : str, default 'assembly_bids'
        The prefix to restrict the files returned by
    bids_bucket_config : bool or str, default False
        If bids_bucket_config is a string, this
        will be used as a config file to identify
        the s3 credentials... otherwise the default
        location will be used
    
    Returns
    -------
    list
        List of file paths that satisfy the requirements
        and that will be used for processing.
        
    '''
    
    # Do some cosmetics so that the prefix includes the pipeline
    # name and so that later we look for the subject ID at the
    # right level
    full_prefix = os.path.join(prefix, subject_id)
    prefix_offset = len(full_prefix.split('/')) - 1
    if len(prefix) == 0:
        prefix_offset = prefix_offset - 1
        
    # Create a PageIterator from the Paginator
    page_iterator = create_page_iterator(bucket = bucket, prefix = full_prefix, bucket_config = bids_bucket_config)

    #Iterate through bucket to find BIDS files for this subject
    subject_files = []
    subject_etags = []
    subject_metadata = []
    subject_full_names = []
    subject_with_slashes = '/' + subject_id + '/'
    for page in page_iterator:
        if 'Contents' in page:
            for temp_dict in page['Contents']:
                skip_file = 0
                partial_path = temp_dict['Key'].split(subject_with_slashes)[-1]
                if type(session) != type(None):
                    if session != partial_path.split('/')[-1].split('_')[1]:
                        for temp_ses_agnostic in session_agnostic_files:
                            if temp_ses_agnostic not in partial_path:
                                skip_file = 1
                
                if skip_file == 0:
                    subject_files.append(partial_path)
                    subject_etags.append(temp_dict['ETag'])
                    subject_metadata.append(temp_dict)
        else:
            print('No BIDS contents found for: {}'.format(subject_id))
    
    sorted_etags = [x for _, x in sorted(zip(subject_files, subject_etags))]
    sorted_etags.reverse()
    sorted_metadata = [x for _, x in sorted(zip(subject_files, subject_metadata))]
    sorted_metadata.reverse()
    sorted_full_names = [x for _, x in sorted(zip(subject_files, subject_full_names))]
    sorted_full_names.reverse()
    subject_files.sort()
    subject_files.reverse()
    output_file_list = []
    etag_dict = {}
    metadata_dict = {}
    for i, parent_requirement in enumerate(requirements_dict.keys()):
        temp_requirement_file_list = []
        temp_requirement_etag_list = []
        temp_requirements_metadata_list = []
        child_requirements = requirements_dict[parent_requirement]['file_naming']
        for j, temp_file in enumerate(subject_files):
            child_requirements_satisfied = 0
            for child_requirement in child_requirements.keys():
                if (child_requirement in temp_file) == child_requirements[child_requirement]:
                    child_requirements_satisfied += 1
                else:
                    break
            if child_requirements_satisfied == len(child_requirements.keys()):
                temp_requirement_file_list.append(temp_file)
                temp_requirement_etag_list.append(sorted_etags[j])
                temp_requirements_metadata_list.append(sorted_metadata[j])
        
        counter = 0
        partial_output_file_list = []
        partial_output_file_list_qc = []
        for j, temp_file in enumerate(temp_requirement_file_list):
            qc_values = []
            requirement_disqualified = 0
            if ('qc_criteria' in requirements_dict[parent_requirement]) and (type(qc_df) != type(None)):
                partial_df = qc_df[qc_df['filename'].str.contains(temp_file.split('/')[-1])]
                if len(partial_df) == 0:
                    is_ses_agnostic = 0
                    for temp_ses_agnostic in session_agnostic_files:
                        if temp_ses_agnostic in temp_file:
                            is_ses_agnostic = 1
                    if is_ses_agnostic == 0:
                        raise NameError('Error: No QC info for {}'.format(temp_file))

                #Iterate through each QC requirement (the requirements are
                #stored as a list of dictionaries, each with one key/value pair)
                for temp_qc_criteria in requirements_dict[parent_requirement]['qc_criteria']:

                    for temp_key in temp_qc_criteria.keys():
                        #If the requirement is a boolean, make sure the
                        #QC value matches that of the requirement
                        if type(temp_qc_criteria[temp_key]) == bool:
                            if partial_df[temp_key].values[0] != temp_qc_criteria[temp_key]:

                                #As backup, check if yes/no was used instead of boolean
                                if temp_qc_criteria[temp_key]:
                                    temp_bool_str = 'YES'
                                else:
                                    temp_bool_str = 'NO'
                                #print(partial_df[temp_key].values[0])
                                #print(type(partial_df[temp_key].values[0]))
                                if type(partial_df[temp_key].values[0]) != str:
                                    temp_val = str(partial_df[temp_key].values[0]).upper()
                                else:
                                    temp_val = partial_df[temp_key].values[0].upper()
                                if temp_val != temp_bool_str:
                                    #print(partial_df[temp_key].values[0])
                                    #print(temp_qc_criteria[temp_key])
                                    #print('{}: {}'.format(temp_key, partial_df[temp_key].values[0]))
                                    requirement_disqualified = 1

                        #Otherwise assume the requirement is a number, and
                        #make sure the observed value is less than the requirement
                        else:
                            if (partial_df[temp_key].values[0] > temp_qc_criteria[temp_key]) or (np.isnan(partial_df[temp_key].values[0])):
                                #print('{}: {}'.format(temp_key, partial_df[temp_key].values[0]))
                                requirement_disqualified = 1
                            else:
                                #If the qc value is good enough, save it so we
                                #can later compare QC values across scans of the
                                #same type and pick the best one
                                qc_values.append(partial_df[temp_key].values[0])
            
            #I think the code below should work whether or not QC information is present
            if requirement_disqualified == 0:
                if 'num_to_keep' not in requirements_dict[parent_requirement].keys():
                    partial_output_file_list.append(temp_file)
                    partial_output_file_list_qc.append(qc_values)
                    etag_dict[temp_file] = temp_requirement_etag_list[j]
                    metadata_dict[temp_file] = temp_requirements_metadata_list[j]
                elif requirements_dict[parent_requirement]['num_to_keep'] > counter:
                    partial_output_file_list.append(temp_file)
                    partial_output_file_list_qc.append(qc_values)
                    etag_dict[temp_file] = temp_requirement_etag_list[j]
                    metadata_dict[temp_file] = temp_requirements_metadata_list[j]
                    counter += 1
                else:
                    #Check which of the existing files has the worst QC values
                    worst_existing_file_qc = partial_output_file_list_qc[0]
                    worst_existing_file_qc_index = 0
                    for qc_ind, temp_qc_partial in enumerate(partial_output_file_list_qc):
                        for temp_qc_item in range(len(partial_output_file_list_qc[0])):
                            #print('temp_qc_partial {}'.format(temp_qc_partial))
                            #print('qc_ind {}'.format(qc_ind))
                            #If QC measure is worse in the current file, then remark this as the worst file
                            if temp_qc_partial[temp_qc_item] > worst_existing_file_qc[temp_qc_item]:
                                worst_existing_file_qc_index = qc_ind
                                worst_existing_file_qc = partial_output_file_list_qc[qc_ind]
                                break
                            #If QC measure is the same in the current file, check the next item
                            elif temp_qc_partial[temp_qc_item] == worst_existing_file_qc[temp_qc_item]:
                                continue
                            #If QC measure is better in the current file, move on to the next file
                            else:
                                break
                                
                    #See if the current file is better/worse than the worst file
                    for temp_qc_item in range(len(worst_existing_file_qc)):
                        
                        #If new file is better, then update the file list with the
                        #current file
                        if worst_existing_file_qc[temp_qc_item] > qc_values[temp_qc_item]:
                            del etag_dict[partial_output_file_list[worst_existing_file_qc_index]]
                            del metadata_dict[partial_output_file_list[worst_existing_file_qc_index]]
                            etag_dict[temp_file] = temp_requirement_etag_list[j]
                            metadata_dict[temp_file] = temp_requirements_metadata_list[j]
                            partial_output_file_list[worst_existing_file_qc_index] = temp_file
                            partial_output_file_list_qc[worst_existing_file_qc_index] = qc_values
                            break
                        #If new file is worse, then move on and ignore the current file
                        elif worst_existing_file_qc[temp_qc_item] > qc_values[temp_qc_item]:
                            break
                        #If the two files are the same, check the next QC file. If the QC values
                        #are the same across the board, the one that has the lowest ranking in
                        #alphabetical order will be chosen.
                        else:
                            continue
        
        output_file_list = output_file_list + partial_output_file_list
        
    #Also find all the various files that are associated with the requirements
    if type(associated_files_dict) != type(None):
        client = create_page_iterator(bucket = bucket, prefix = full_prefix, bucket_config = bids_bucket_config, return_client_instead = True)
        
        # Create a reusable Paginator
        paginator = client.get_paginator('list_objects')
        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
        new_files = []
        for temp_file in output_file_list:
            for temp_key in associated_files_dict.keys():
                if temp_key in temp_file:
                    for temp_term in associated_files_dict[temp_key]:
                        temp_file_path = os.path.join(prefix, subject_id, temp_file.replace(temp_key, temp_term))
                        page_iterator = paginator.paginate(Bucket=bucket, Prefix=temp_file_path)
                        for page in page_iterator:
                            if 'Contents' in page:
                                for temp_dict in page['Contents']:
                                    new_file_name = temp_dict['Key'].split(subject_with_slashes)[-1]
                                    new_files.append(new_file_name)
                                    etag_dict[new_file_name] = temp_dict['ETag'].split(subject_with_slashes)[-1]
                                    metadata_dict[new_file_name] = temp_dict

        output_file_list = list(set(output_file_list + new_files))
        output_file_list.sort()

    for a in metadata_dict.keys():
        for b in metadata_dict[a].keys():
             if isinstance(metadata_dict[a][b], datetime.datetime):
                metadata_dict[a][b] = metadata_dict[a][b].isoformat()

    return output_file_list, metadata_dict

def is_qc_info_required(requirement_dictionary):
    '''Return True if QC info is required for any given requirement'''
    
    for temp_req in requirement_dictionary.keys():
        if 'qc_criteria' in requirement_dictionary[temp_req]:
            return True
    
    return False

def register_cbrain_csvs_in_cbrain(file_name, cbrain_api_token, user_id = 4022, group_id = 10367, browse_path = "cbrain_misc/cbrain_csvs", data_provider_id = 710):
    '''Register CBRAIN CSVs in CBRAIN

    This function will try to register a CBRAIN
    CSV file from your S3 data provider in CBRAIN.
    Note - if the file already exists in CBRAIN,
    the registration will fail. Even if the upload
    is unsuccessful, it will probably finish without
    error'ing out. Look at the printed outputs for
    more detail.
    
    Parameters
    ----------
    file_name : str
        The name of the CBRAIN CSV file
        in your DataProvider that needs
        to get registred in CBRAIN
    cbrain_api_token : str
        The api token generated when you logged into cbrain
    user_id : int, default 4022
        Your CBRAIN user ID, by default 4022
    group_id : int, default 10367
        The corresponding group/project ID in cbrain, by default 10367
    browse_path : str, default "cbrain_misc/cbrain_csvs"
        The directory where CBRAIN CSV files are stored
        on the S3 data provider
    data_provider_id : int, default 710

    '''

    print('Attempting to register file {}'.format(file_name))

    data = {
    "basenames": [file_name], #name of file
    "filetypes": ["ExtendedCbrainFileList-{}".format(file_name)], #filetype - name of file
    "as_user_id": user_id, #my user id
    "browse_path" : browse_path, #this folder is in base layer of dp
    "other_group_id": group_id #loris abcd group
        }

    base_url = 'https://portal.cbrain.mcgill.ca'
    task_params = (
        ('cbrain_api_token', cbrain_api_token),
    )

    dp_response = requests.post(
        url = '/'.join([base_url, 'data_providers', str(data_provider_id), 'register']),
        headers = {'Accept': 'application/json'},
        params = task_params,
        data = data
    )

    dp_response.json()
    if dp_response.status_code == 200:
        print('CBRAIN Notice: {}'.format(dp_response.json()['notice']))
        print('CBRAIN Error: {}'.format(dp_response.json()['error']))
    else:
        print("Registration of {} failed.".format(file_name))
        print(dp_response.text)
                      
    return

def download_scans_tsv_file(bucket_config, output_folder, subject, session, bids_prefix = 'assembly_bids', bucket = 'hbcd-pilot', client = None):
    '''Download scans.tsv file for a given subject/session
    
    Parameters
    ----------
    
    bucket_config : str
        This will be used as a config file to identify
        the s3 credentials
    output_folder : str
        Where to save the downloaded file
    subject : str
        Name of subject
    session : str
        Name of session
    bids_prefix : str, default 'assembly_bids'
        The path to the BIDS study directory
    bucket : str, default 'hbcd-pilot'
        The bucket where the file to download is
    client : existing boto3 client, or None, default None
        Option to use an existing boto3 client instead
        of creating a new one
        
    Returns
    -------
    
    Path to downloaded scans.tsv file if download was successful,
    otherwise returns None
        
    '''

    # Make a new s3 client if it doesn't exist   
    if type(client) == type(None):
        client = create_boto3_client(s3_config = bucket_config)

    #Iterate through bucket to find potential subjects
    file_to_download = os.path.join(bids_prefix, subject, session, '{}_{}_scans.tsv'.format(subject,session))
    downloaded_file = os.path.join(output_folder, file_to_download.split('/')[-1])
    try:
        client.download_file(bucket, file_to_download, downloaded_file)
    except:
        return None
            
    return downloaded_file

def create_boto3_client(s3_config = None):
    '''Utility to create a boto3 client
    
    Parameters
    ----------
    
    s3_config : str or None, default None
        Path to s3 configuration file
        
    Returns
    -------
    
    boto3 client
    
    '''
    
    #Grab config path
    if s3_config == False:
        config_path = ''
    else:
        if type(s3_config) != str:
            raise NameError('Error: different config path should eithe be string or boolean')
        else:
            config_path = s3_config
            
    #Find info from config file    
    with open(config_path, 'r') as f:
        lines = f.read().splitlines()
        for temp_line in lines:
            if 'access_key' == temp_line[:10]:
                access_key = temp_line.split('=')[-1].strip()
            if 'secret_key' == temp_line[:10]:
                secret_key = temp_line.split('=')[-1].strip()
            if 'host_base' == temp_line[:9]:
                host_base = temp_line.split('=')[-1].strip()
                if 'https' != host_base[:5]:
                    host_base = 'https://' + host_base
        
    #Create s3 client
    client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url =host_base
    )
    
    return client

def cbrain_mark_as_newer(file_id, cbrain_api_token):
    '''Mark file as newer on CBRAIN
    
    If a file is cached in cbrain, you can call
    this function to make sure the cache is updated
    before the file is used in any upcoming processing.
    This means the file will be re-downloaded from the
    data provider where the file lives. If mark as newer
    has already been called then CBRAIN will say 0 files
    were marked as newer. This same behavior is also
    observed in the case where the file is on the data
    provider but not yet cached on the system where processing
    occurs.
    
    Parameters
    ----------
    file_id : str
        The id of the CBRAIN file that
        should be "marked as newer"
    cbrain_api_token : str
        The api token generated when you logged into cbrain

    '''    
    
    data = {
              "file_ids": [file_id],
              "operation": "all_newer",
            }
    
    
    base_url = 'https://portal.cbrain.mcgill.ca'
    task_params = (
        ('cbrain_api_token', cbrain_api_token),
    )
    
    dp_response = requests.post(
        url = '/'.join([base_url, 'userfiles', 'sync_multiple']),
        headers = {'Accept': 'application/json'},
        params = task_params,
        json = data
    )
    

    dp_response.json()
    if dp_response.status_code == 200:
        print('    CBRAIN Notice: {}'.format(dp_response.json()['notice']))
    else:
        print("    Mark As Newer for {} failed.".format(file_id))
        print(dp_response.text)
                      
    return
    
    
def check_if_derivatives_exist(subject_name, pipeline_folder, bucket = 'hbcd-pilot', prefix = 'derivatives', derivatives_bucket_config = False):
    '''Utility to check if a subject has specific BIDs derivatives
    
    Parameters
    ----------
    
    subject_name : str
        The name of the subject whose output
        you want to check (i.e. sub-01)
    pipeline_folder : str
        The name of the pipeline folder
        found under prefix (i.e. mriqc)
    bucket : str, default 'hbcd-pilot'
        The name of the bucket to query for files
    prefix : str, default 'derivatives'
        The prefix to restrict the files returned by
        the search query 
    bids_bucket_config : bool or str, default False
        If bids_bucket_config is a string, this
        will be used as a config file to identify
        the s3 credentials... otherwise the default
        location will be used
        
    Returns
    -------
    
    bool : True if any S3 files are found for the
           bucket/prefix/pipeline/subject combo,
           otherwise False
        
    '''

    # Do some cosmetics so that the prefix includes the pipeline
    # name and so that later we look for the subject ID at the
    # right level
    full_prefix = os.path.join(prefix, pipeline_folder, subject_name)
    page_iterator = create_page_iterator(bucket = bucket, prefix = full_prefix, bucket_config = derivatives_bucket_config)
    for page in page_iterator:
        if 'Contents' in page:
            if len(page['Contents']):
                return True
        else:
            return False
    
def grab_json(json_config_location, pipeline_name, session_label = None):
    """Load json config for a given pipeline

    Parameters
    ----------
    json_config_location : str or False, default False
        If the json config file is stored somewhere outside
        of the processing_configurations folder, you can
        specify the path to the json file here. Otherwise,
        the file will be found in the processing_configurations
        folder.
    pipeline_name : str
        The name of the pipeline whose json config
        you want to load.

    Returns
    -------
    json_contents : dict

    """
    
    #hello
    
    #Grab the json config path and load it
    if json_config_location != False:
        json_config_location = json_config_location
    else:
        #Grab config file from processing_configurations folder which should be in the same directory as this script
        #Later will need to get rid of HBCD_CBRAIN_PROCESING reference
        json_config_location = os.path.join(Path(inspect.getfile(update_processing)).absolute().parent.resolve(), 'processing_configurations', '{}.json'.format(pipeline_name))
        if os.path.exists(json_config_location) == False:
            raise NameError('Error: expected json configuration for {} at {}'.format(pipeline_name, json_config_location))
    with open(json_config_location, 'r') as f:
        json_contents = json.load(f)

    #Also if the subject specified a session for processing,
    #load the session_arguments.json so that a session argument
    #can be passed to the pipeline if we have one on record.
    if type(session_label) != type(None):
        ses_config_location = os.path.join(Path(inspect.getfile(update_processing)).absolute().parent.resolve(), 'session_arguments.json')
        with open(ses_config_location, 'r') as f:
            ses_json_contents = json.load(f)
        if pipeline_name in ses_json_contents.keys():
            json_contents[ses_json_contents[pipeline_name]] = session_label
            
    return json_contents


def construct_generic_cbrain_task_info_dict(cbrain_api_token, group_id, user_id, tool_config_id, data_provider_id, task_description, variable_parameters_dict, fixed_parameters_dict, all_to_keep = None):
    """Constructs dictionaries needed to launch CBRAIN task
    
    Parameters
    ----------

    cbrain_api_token : str
        The api token generated when you logged into cbrain
    group_id : str
        The group ID for the project you want to run the task on
    user_id : str
        The user ID for the user who will be running the task
    tool_config_id : str
        The tool config ID for the tool you want to run
    data_provider_id : str
        The data provider ID for the data provider you want to
        store the results in
    task_description : str
        A description of the task that will be included in the
        CBRAIN task summary (viewable from web interface)
    variable_parameters_dict : dict
        A dictionary of the variable parameters for the task
        (i.e. the parameters that will be changed for each
        subject)
    fixed_parameters_dict : dict
        A dictionary of the fixed parameters for the task
        (i.e. the parameters that will be the same for each
        subject)
    all_to_keep : list or None, default None
        A list of the files you want to keep from the subject
        directory for processing. If None, all files will be
        kept. If a list, only the files in the list will be
        kept. If a list, the files that will be removed are
        ONLY files that are in the same folder as a file that
        is specified in the list. So if you say you only want
        some T1 file from the anat dir, it will remove other
        files from the anat dir, but will leave the func dir,
        or other session dirs untouched.

    Returns
    -------

    task_headers : dict
        The headers needed to launch a CBRAIN task
    task_params : dict
        The parameters needed to launch a CBRAIN task
    task_data : dict
        The data needed to launch a CBRAIN task
    """
    
    task_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }
    task_params = (
        ('cbrain_api_token', cbrain_api_token),
    )

    task_data = { 
        'cbrain_task': {
            'group_id': group_id,
            'user_id' : user_id,
            'tool_config_id': tool_config_id,
            'results_data_provider_id': data_provider_id,
            'description': task_description,
            'params': {}
        }
    }
    
    task_data['cbrain_task']['params']['invoke'] = fixed_parameters_dict
    
    userfile_ids = []
    for temp_key in variable_parameters_dict:
        temp_val = variable_parameters_dict[temp_key]
        if type(temp_val) != str:
            temp_val = str(temp_val)
        task_data['cbrain_task']['params']['invoke'][temp_key] = temp_val #these will be different arguments to the task that
        userfile_ids.append(temp_val)                                     #are generally different across subjects (such as the subject i)
        
    #Specify all of the userfile ids
    task_data['cbrain_task']['params']['interface_userfile_ids'] = userfile_ids
    #Tell CBRAIN to only keep files in all_to_keep array. The function of this
    #is complicated though... it will only remove non-listed files if there is a
    #listed file within the same folder
    if type(all_to_keep) != type(None):
        task_data['cbrain_task']['params']['invoke']['all_to_keep'] = all_to_keep
    
    return task_headers, task_params, task_data

def submit_generic_cbrain_task(task_headers, task_params, task_data, pipeline_name):
    '''Function to submit CBRAIN task via API

    This is only used by launch_task_concise_dict.
    
    Parameters
    ----------
    
    task_headers : dict
        generated by construct_generic_cbrain_task_info_dict(dict)
    task_params : dict
        generated by construct_generic_cbrain_task_info_dict(dict)
    task_data : dict
        generated by construct_generic_cbrain_task_info_dict(dict)
    pipeline_name : str
        name of pipeline (used only for troubleshooting)
        
    Returns
    -------
    
    bool, True if CBRAIN accepts request
        
    '''
    
    task_response = requests.post(
        url = '/'.join(['https://portal.cbrain.mcgill.ca', 'tasks']),
        headers = task_headers,
        params = task_params,
        data = json.dumps(task_data)
    )

    if task_response.status_code == 200:
        #print("Successfully submitted {} processing to CBRAIN for CBRAIN CSV File.".format(pipeline_name))
        task_info = task_response.json()
        #print(json.dumps(task_info, indent=4))
        json_for_logging = {}
        json_for_logging['returned_by_cbrain'] = task_info
        json_for_logging['submitted_task_headers'] = task_headers
        json_for_logging['submitted_task_data'] = task_data
        return True, json_for_logging
    else:
        print("Failed to submit {} processing to CBRAIN for CBRAIN CSV File with ID.".format(pipeline_name))
        print("Task Data of failed task: {}".format(task_data))
        print(task_response.text)
        return False, {}
    


def launch_task_concise_dict(pipeline_name, variable_parameters_dict, cbrain_api_token,
                             data_provider_id = 710, override_tool_config_id = False,
                             group_id = 10367, user_id = 4022, task_description = '',
                             custom_json_config_location = False, all_to_keep = None,
                             session_label = None):

    '''Uses submit_generic_cbrain_task to launch processing
    


    Function that uses a cbraincsv file to launch processing on CBRAIN
    
    This function is used to launch MRIQC processing on CBRAIN given the ID
    of a CBRAIN CSV File/Extended File List and a number of other fields.
    
    Parameters
    ----------
    
    
    pipeline_name : str
        Corresponds to the pipeline configurations in the git repo
    variable_parameters_dict : dict
        A dictionary of the variable parameters for the task
        (i.e. the parameters that will be changed for each
        subject)
    cbrain_api_token : str
        The API token for your current cbrain session (needs to be refreshed every day or
        so from the CBRAIN website)
    data_provider_id : str, default = '710' (for hbcd-pilot)
        The data provider ID for the data provider you want to
        store the results in
    override_tool_config_id : bool or str, default False
        The numeric id of the tool on a particular system. For example 4331 represents a
        specific version of QSIPREP on UMN/MSI's Mesabi system. By default this will be read
        from the tool_config_ids file unless a different tool_config_id is specified.
    group_id : str, default '10367'
        The CBRAIN permissions group associated with the current run
    user_id : str, default '4022'
        The CBRAIN user id associated with the current run
    task_description : str, default ''
        A description of the task that will be displayed on the CBRAIN website
    custom_json_config_location : bool or str, default False
        The location of the json config file that contains the fixed parameters for the task.
        By default this will be read from the json_configs file unless a different location
        is specified.
    all_to_keep : list or None, default None
        A list of the files you want to keep from the subject
        directory for processing. If None, all files will be
        kept. If a list, only the files in the list will be
        kept. If a list, the files that will be removed are
        ONLY files that are in the same folder as a file that
        is specified in the list. So if you say you only want
        some T1 file from the anat dir, it will remove other
        files from the anat dir, but will leave the func dir,
        or other session dirs untouched.
        
    Returns
    -------
    bool
        True if task is submitted to CBRAIN without immediate error, else False
        
    '''
    tool_config_file = os.path.join(Path(inspect.getfile(update_processing)).absolute().parent.resolve(), 'tool_config_ids.json')
    with open(tool_config_file, 'r') as f:
        tool_config_dict = json.load(f)
    tool_config_id = str(tool_config_dict[pipeline_name])
    
    #Grab the json config path and load it
    #json_contents = grab_json(custom_json_config_location, pipeline_name)
    fixed_parameters_dict = grab_json(custom_json_config_location,
                                      pipeline_name,
                                      session_label = session_label)
        
    #Construct different dictionaries that will be sent to CBRAIN
    task_headers, task_params, task_data = construct_generic_cbrain_task_info_dict(cbrain_api_token, group_id, user_id, tool_config_id, data_provider_id, task_description, variable_parameters_dict, fixed_parameters_dict, all_to_keep = all_to_keep)
        
    #Submit task to CBRAIN
    status, json_for_logging = submit_generic_cbrain_task(task_headers, task_params, task_data, pipeline_name)
    return status, json_for_logging


def find_current_cbrain_tasks(cbrain_api_token, data_provider_id = None):
    '''Generates info on extended file list files
    
    Parameters
    ----------
    
    cbrain_api_token : str
        The CBRAIN API token for the current session
    data_provider_id : None or int
        Restrict tasks to the specific data provider
        
    Returns
    -------
    list of dictionaries with info on current CBRAIN tasks
    
    '''
    base_url = 'https://portal.cbrain.mcgill.ca'
    tasks = []
    tasks_request = {'cbrain_api_token': cbrain_api_token, 'page': 1, 'per_page': 1000}

    while True:
        tasks_response = requests.get(
            url = '/'.join([base_url, 'tasks']),
            data = tasks_request,
            headers = {'Accept': 'application/json'}
        )
        if tasks_response.status_code != requests.codes.ok:
            print('User tasks request failed.')
            print(tasks_response)
            break
        # Collect the responses on this page then increment
        tasks += tasks_response.json()
        tasks_request['page'] += 1
        # Stop requesting responses when we're at the last page
        if len(tasks_response.json()) < tasks_request['per_page']:
            break 
            
    if type(data_provider_id) == type(None):
        return tasks
    else:
        if type(data_provider_id) == str:
            data_provider_id = int(data_provider_id)
        tasks_to_return = []
        for temp_task in tasks:
            if temp_task['results_data_provider_id'] == data_provider_id:
                tasks_to_return.append(temp_task)
        return tasks_to_return


def find_cbrain_extended_file_list_files(cbrain_api_token, data_provider_id = 710):
    '''Finds existing CBRAIN CSV files
    
    Finds CBRAIN CSV files/Extended File Lists. This list will be used
    to show which subjects processing has already been attempted on.
    
    Parameters
    ----------
    
    cbrain_api_token : str
        The CBRAIN API token for the current session
    data_provider_id : int
        Data provider ID to restrict files to
        
    Returns
    -------
    list of dictionaries with info on CBRAIN CSV files
    
    '''
    base_url = 'https://portal.cbrain.mcgill.ca'
    files = []
    files_request = {'cbrain_api_token': cbrain_api_token, 'page': 1, 'per_page': 1000}

    while True:
        files_response = requests.get(
            url = '/'.join([base_url, 'userfiles']),
            data = files_request,
            headers = {'Accept': 'application/json'}
        )
        if files_response.status_code != requests.codes.ok:
            print('User files request failed.')
            print(files_response)
            break
        # Collect the responses on this page then increment
        files += files_response.json()
        files_request['page'] += 1
        # Stop requesting responses when we're at the last page
        if len(files_response.json()) < files_request['per_page']:
            break 
            
    #Restrict the returned files to ones in the current DP and of type ExtendedCbrainFileList
    filter_results = list(filter(lambda f: 'ExtendedCbrainFileList' == f['type'], files))
    filter_results = list(filter(lambda f: data_provider_id == f['data_provider_id'], filter_results))

    #Print out info about what files were found
    print("{} total files found\n".format(str(len(files))))
    print("{} Extended File List files found under DP {}".format(len(filter_results), data_provider_id))
    
    return filter_results

def find_cbrain_files_on_dp(cbrain_api_token, data_provider_id = 710):
    '''Returns list of files on data provider
    
    Parameters
    ----------
    
    cbrain_api_token : str
        The CBRAIN API token for the current session
    data_provider_id : int
        Data provider ID to restrict files to
        
    Returns
    -------
    list of dictionaries with info on all CBRAIN files
    existing on the specified data provider
    
    '''
    base_url = 'https://portal.cbrain.mcgill.ca'
    files = []
    files_request = {'cbrain_api_token': cbrain_api_token, 'page': 1, 'per_page': 1000}
    data_provider_id = int(data_provider_id)

    while True:
        files_response = requests.get(
            url = '/'.join([base_url, 'userfiles']),
            data = files_request,
            headers = {'Accept': 'application/json'}
        )
        if files_response.status_code != requests.codes.ok:
            print('User files request failed.')
            print(files_response)
            break
        # Collect the responses on this page then increment
        files += files_response.json()
        files_request['page'] += 1
        # Stop requesting responses when we're at the last page
        if len(files_response.json()) < files_request['per_page']:
            break 
            
    #Restrict the returned files to ones in the current DP and of type ExtendedCbrainFileList
    filter_results = list(filter(lambda f: data_provider_id == f['data_provider_id'], files))

    #Print out info about what files were found
    print("{} total files found under data provider {}\n".format(len(filter_results), data_provider_id))
    
    return filter_results


def grab_external_requirements(subject_name, cbrain_files, 
                                requirements_dict,
                                bids_data_provider_id = None,
                                derivatives_data_provider_id = None):
    '''Grab's external requirements for a subject

    External requirements are either non-BIDS files
    (such as outputs from other pipelines) or BIDS
    files found on CBRAIN that will be used for processing.

    For example, if the requirements_dict below was used,
    this function would look for a file on CBRAIN with type
    BidsSubject and the name passed as subject_name and
    then return a dictionary that is something like
    {'subject_dir' : 12342435} where the number is the
    id of the file on CBRAIN. If no BidsSubject is found,
    then this returns None.
    
    requirementa_dict = {"subject_dir" : "BidsSubject"}
    
    Parameters
    ----------
    subject_name : str
        Subject name (i.e. sub-001)
    cbrain_files : list of dicts
        Files on the data provider of interest. This
        can be a mix of bids/derivatives DP files if
        both bids_data_provider_id and derivatives_data_provider_id
        are specified.
    requirements_dict : dict
    bids_data_provider_id : int or None, default None
        If specified, this will restrict the search for
        BIDS files to the specified data provider
    derivatives_data_provider_id : int or None, default None
        If specified, this will restrict the search for
        derivatives files to the specified data provider.
        Any non-numeric requirements (i.e. file types) that
        are not BidsSubjects will be assumed to be derivatives
    
    Returns
    -------
        None if some requirements from requirements_dict were not
        found, otherwise returns a dictionary with the ids for the
        different files
    
    
    '''

    #First create this dictionary that will be used to track why certain
    #subjects are processed and others arent processed. Used only for tracking,
    #not for determining who is processed.
    requirements_tracking_dict = {}
    
    subject_external_requirements = {}
    for temp_requirement in requirements_dict.keys():
        requirement_found = False
        #If the requirement is numeric, then this refers to a CBRAIN file ID
        if requirements_dict[temp_requirement].isnumeric():
            subject_external_requirements[temp_requirement] = requirements_dict[temp_requirement]
        
        #Otherwise, we will look for a CBRAIN file with the specified file type and with the subject
        #name
        else:
            for temp_file in cbrain_files:
                if (temp_file['name'] == subject_name) and (temp_file['type'] == requirements_dict[temp_requirement]):
                    #Dont use the file if (1) the bids data provider is specified
                    # (2) the file is a BIDS subject and (3)
                    # the file comes from a non-BIDS DP
                    if type(bids_data_provider_id) != type(None):
                        if (temp_file['type'] == 'BidsSubject') and (bids_data_provider_id != temp_file['data_provider_id']):
                            continue

                    #Dont use the file if (1) the deriv data provider is specified
                    # (2) the file is not a BIDS subject and (3)
                    # the file comes from the BIDS DP
                    if type(derivatives_data_provider_id) != type(None):
                        if (temp_file['type'] != 'BidsSubject') and (bids_data_provider_id == temp_file['data_provider_id']):
                            continue
                    requirement_found = True
                    subject_external_requirements[temp_requirement] = temp_file['id']
                    requirements_tracking_dict[temp_requirement] = 'Satisfied'
                    break
            if requirement_found == False:
                requirements_tracking_dict[temp_requirement] = 'No File'
                print('Requirement {} not found for subject {}'.format(temp_requirement, subject_name))
                return None, requirements_tracking_dict

    return subject_external_requirements, requirements_tracking_dict

def find_potential_subjects_for_processing(cbrain_api_token, bids_bucket_config, bids_bucket = 'hbcd-pilot',
                                           bids_prefix = 'assembly_bids', data_provider_id = '710'):
    """Find subjects that may be ready for processing
    
    Looks for subjects that are already registered in CBRAIN
    and also exist in the S3 bucket. Returns arguments that
    will be used with update_fully_generic_processing_using_csv_dict
    to run processing with different pipelines.
    
    Parameters
    ----------

    cbrain_api_token : str
        The CBRAIN API token for the current session
    bids_bucket_config : str
        Path to the bids bucket config file
    bucket : str
        Name of the S3 bucket to look for subjects in
    prefix : str
        Prefix to look for subjects in (i.e. assembly_bids)
    data_provider_id : str
        CBRAIN Data Provider ID to look for subjects in
    
    Returns
    -------

    registered_and_s3 : list
        Subjects that are registered in CBRAIN and in S3
    registered_and_s3_ids : list
        CBRAIN IDs for the subjects that are registered in CBRAIN and in S3
    registered_and_s3_sizes : list
        Sizes of the subjects's file collections (taken
        from CBRAIN)

    """

    #Find S3 Subjects
    s3_subjects = find_s3_subjects(bids_bucket_config, bucket = bids_bucket, prefix = bids_prefix)

    #Grab data provider id for bucket from cbrain web instance
    cbrain_subjects, ids, sizes = find_cbrain_subjects(cbrain_api_token, data_provider_id = data_provider_id) #710 = HBCD Pilot Official, #725 is old

    total_subjects = set(cbrain_subjects + s3_subjects)
    non_registered_subjects = set(s3_subjects) - set(cbrain_subjects)
    missing_in_s3_subjects = set(total_subjects) - set(s3_subjects)
    registered_and_s3 = (total_subjects - missing_in_s3_subjects) - non_registered_subjects
    print('Total identified subjects: {},\nSubjects Still Needing to Be Registered in CBRAIN: {},\nSubjects Registered in CBRAIN and in S3 (ready to process): {},\nRegistered Subjects Not in S3 (this should be 0): {}\n\n'.format(len(total_subjects), len(non_registered_subjects), len(registered_and_s3), len(missing_in_s3_subjects)))

    registered_and_s3_ids = []
    registered_and_s3_names = []
    registered_and_s3_sizes = []
    for temp_subject in registered_and_s3:
        for i, temp_cbrain in enumerate(cbrain_subjects):
            if temp_cbrain == temp_subject:
                registered_and_s3_ids.append(ids[i])
                registered_and_s3_names.append(temp_cbrain)
                registered_and_s3_sizes.append(sizes[i])
                break

    return registered_and_s3_names, registered_and_s3_ids, registered_and_s3_sizes

def check_rerun_status(cbrain_subject_id, cbrain_tasks, derivatives_data_provider_id, tool_config_id, rerun_level = 1):
    '''Function that helps determine whether processing should be ran based on existing CBRAIN tasks.
    
    Parameters
    ----------
    cbrain_subject_id : str
        Numeric ID of CBRAIN subject or file. NOTE
        if processing was launched with a CBRAIN CSV,
        then the CBRAIN CSV's ID may need to be used
        instead.
    cbrain_tasks : list of dicts
        All tasks in CBRAIN (returned by CBRAIN API)
    derivatives_data_provider_id : int
        The data provider being used for processing
    tool_config_id : int
        The id of the tool config being used for processing
    rerun_level : 0, 1, 2, default 1
        This determines when the function will recommend rerunning
        processing. If 0 is used, processing will only be recommended
        when the cbrain_subject_id hasn't been used in any cbrain_tasks
        with the specified derivatives_data_provider_di and tool_config_id.
        If 1 is used, processing will also be recommended when CBRAIN tasks
        fall under certain states that indicate processing failed for CBRAIN/
        system related reasons. If 2 is used, processing will also be recommended
        when CBRAIN tasks have failed due to processing reasons. The function
        should never recommend processing when there is a corresponding task that's
        currently running, being setup, or in a similar processing state.
        
    Returns
    -------
    -True if processing is recommended, else False
    -Example status from the tasks that were found. Prioritizes statuses
    from rerun_group_2. Also gives custom status of "No Tasks Found" if
    no associated tasks exist.
    
    
    '''

    rerun_group_1 = ['Terminated', 'Failed To Setup', 'Failed To PostProcess', 'Failed Setup Prerequisites', 'Failed PostProcess Prerequisites']
    rerun_group_2 = ['Suspended', 'Failed', 'Failed On Cluster']
    cbrain_subject_id_str = str(cbrain_subject_id)

    task_statuses = []
    task_ids = []
    for temp_task in cbrain_tasks:
        if temp_task['tool_config_id'] == int(tool_config_id):
            if temp_task['results_data_provider_id'] == int(derivatives_data_provider_id):
                try:
                    if cbrain_subject_id_str in temp_task['params']['interface_userfile_ids']:
                        task_statuses.append(temp_task['status'])
                        task_ids.append(temp_task['id'])
                except:
                    continue

    num_rerun_group_1 = 0
    num_rerun_group_2 = 0
    example_status = None
    for temp_status in task_statuses:
        if temp_status in rerun_group_1:
            num_rerun_group_1 += 1
            example_status = temp_status
        if temp_status in rerun_group_2:
            num_rerun_group_2 += 1
            if type(example_status) == type(None):
                example_status = temp_status
        if type(example_status) == type(None):
            example_status = temp_status

    if len(task_statuses) == 0:
        example_status = 'No Tasks Found'
        return True, example_status
    else:
        if rerun_level == 0:
            print('    Found existing task(s), consider deleting the task(s) or using higher rerun level. Tasks: {}, Statuses: {}'.format(task_ids, task_statuses))
        elif rerun_level == 1:
            if len(task_statuses) == num_rerun_group_1:
                return True, example_status
            else:
                print('    Found existing task(s) with status within rerun_level = 1, consider deleting the task(s) or using higher rerun level. Tasks: {}, Statuses {}'.format(task_ids, task_statuses))
        elif rerun_level == 2:
            if len(task_statuses) == (num_rerun_group_1 + num_rerun_group_2):
                return True, example_status
            else:
                print('    Found existing task(s) with status within rerun_level = 2, consider deleting the task(s) or using higher rerun level. Tasks: {}, Statuses {}'.format(task_ids, task_statuses))
        else:
            raise ValueError('Error: rerun_level must be 0, 1, or 2')

    return False, example_status

def color_specific_value_cells(styler, df, primary_column, value="specific_value", color="red", secondary_column=None):
    """
    Applies color to cells in the secondary column of a Styler object based on conditions met in the primary column
    of the provided DataFrame. If no secondary column is specified, applies coloring to the primary column.

    Parameters:
    - styler: pandas.io.formats.style.Styler, the Styler object of the DataFrame to apply styles to.
    - df: pandas.DataFrame, the DataFrame used to determine the styling conditions.
    - primary_column: str, the name of the primary column to check the condition against.
    - value: str, the value to check for in the primary column.
    - color: str, the CSS color to apply to cells in the secondary column based on the primary column's condition.
    - secondary_column: str, optional, the name of a secondary column where the color is applied based on the primary column's condition.

    Returns:
    - pandas.io.formats.style.Styler, the updated Styler object with applied styles.
    """
    # Create a mask based on the condition in the primary column
    mask = df[primary_column] == value

    # Define the function to apply styles
    def apply_styles(row):
        if row.name in df.index:
            primary_condition = mask.iloc[row.name]
            colors = [''] * len(row)  # Initialize with no coloring
            if primary_condition:
                if secondary_column:  # Apply color to the secondary column based on primary condition
                    colors[df.columns.get_loc(secondary_column)] = f'background-color: {color}'
                else:  # Or apply to the primary column if no secondary is specified
                    colors[df.columns.get_loc(primary_column)] = f'background-color: {color}'
        return colors

    # Apply the function across the DataFrame, row-wise
    updated_styler = styler.apply(apply_styles, axis=1)
    
    return updated_styler


def add_title_and_list_to_html_content(html_content, title, list_of_strings):
    """
    Modifies HTML content to include a title and a series of bullet points.

    Parameters:
    - html_content: str, the HTML content as a string.
    - title: str, the title to be added to the HTML.
    - list_of_strings: list, a list of strings to be converted into bullet points.

    Returns:
    - str, the modified HTML content with added title and bullet points.
    """
    # Create the title and bullet points HTML
    title_html = f'<h1>{title}</h1>\n'
    list_html = '<ul>\n' + ''.join([f'<li>{item}</li>\n' for item in list_of_strings]) + '</ul>\n'
    
    # Insert the title and list HTML before the table
    # Assuming the first occurrence of <table relates to the DataFrame's table
    table_index = html_content.find('<table')
    if table_index == -1:
        # If no table is found, prepend the title and list to the HTML content
        modified_html = title_html + list_html + html_content
    else:
        modified_html = html_content[:table_index] + title_html + list_html + html_content[table_index:]
    
    return modified_html


def add_prettier_background_to_html(html_content):
    """
    Modifies the given HTML content to include prettier background formatting using CSS.

    Parameters:
    - html_content: str, the HTML content as a string.

    Returns:
    - str, the modified HTML content with additional CSS for prettier formatting.
    """
    # Define the CSS for prettier background and overall formatting
    css_styles = """
    <style>
    body {
        font-family: Arial, sans-serif;
        background-color: #f0f0f0;
        margin: 20px;
        padding: 20px;
    }
    table {
        border-collapse: collapse;
        width: 100%;
    }
    th, td {
        text-align: left;
        padding: 8px;
    }
    tr:nth-child(even) {
        background-color: #dddddd;
    }
    th {
        background-color: #4CAF50;
        color: white;
    }
    h1 {
        color: #333;
    }
    ul {
        list-style-type: square;
    }
    li {
        margin-bottom: 5px;
    }
    </style>
    """

    # Insert the CSS at the beginning of the HTML content
    html_with_css = css_styles + html_content

    return html_with_css

def append_pie_charts_to_html(html_str, data_dict):
    """
    Appends high-resolution pie charts to the end of an HTML string based on a provided dictionary.
    Each chart has a title, and the legend shows the count for each category.

    Parameters:
    - html_str: A string containing HTML.
    - data_dict: A dictionary with titles of plots and data for the pie charts.

    Returns:
    - A modified HTML string with pie charts appended.
    """

    # Start the section for pie charts in the HTML
    new_html = html_str + "\n<div id='pie-charts'>\n"

    for title, data in data_dict.items():
        # Prepare data for the pie chart
        labels = list(data.keys())
        sizes = list(data.values())
        # Create labels with counts for the legend
        legend_labels = [f"{label} ({size})" for label, size in zip(labels, sizes)]

        # Create a pie chart
        fig, ax = plt.subplots()
        wedges, texts = ax.pie(sizes, startangle=90, counterclock=False)
        ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        plt.title(title)
        ax.legend(wedges, legend_labels, title="Categories", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))

        # Convert the plot to a PNG image, then encode it in base64 at high resolution
        img_data = BytesIO()
        plt.savefig(img_data, format='png', dpi=150, bbox_inches="tight")
        plt.close()  # Close the plot to free memory
        img_data.seek(0)  # Go to the beginning of the BytesIO buffer
        img_base64 = base64.b64encode(img_data.read()).decode('utf-8')

        # Embed the base64-encoded PNG image into the HTML
        img_html = f"<img src='data:image/png;base64,{img_base64}' alt='{title}'><br>\n"
        new_html += img_html

    # Close the pie charts section
    new_html += "</div>\n"

    return new_html

def reformat_df_and_produce_proc_html(study_tracking_df, pipeline_name, ses_label, output_html_path, file_selection_dict):
    
    study_tracking_df = study_tracking_df.sort_values(by=['subject'])
    study_tracking_df.reset_index(drop=True, inplace=True)
    
    study_tracking_df.fillna('Not Evaluated', inplace=True)
    study_tracking_df.loc[study_tracking_df["scans_tsv_present"] == 1, "scans_tsv_present"] = 'True'
    study_tracking_df.loc[study_tracking_df["scans_tsv_present"] == 0, "scans_tsv_present"] = 'False'
    study_tracking_df.loc[study_tracking_df["derivatives_found"] == 1, "derivatives_found"] = 'True'
    study_tracking_df.loc[study_tracking_df["derivatives_found"] == 0, "derivatives_found"] = 'False'
    styler = study_tracking_df.style
    for temp_key in file_selection_dict.keys():
        styler = color_specific_value_cells(styler, study_tracking_df, temp_key, 'Satisfied', color='Green')
        styler = color_specific_value_cells(styler, study_tracking_df, temp_key, 'Failed QC', color='Red')
        styler = color_specific_value_cells(styler, study_tracking_df, temp_key, 'No File', color='Yellow')


    for temp_col in study_tracking_df.columns:
        styler = color_specific_value_cells(styler, study_tracking_df, temp_col, 'Not Evaluated', color='Grey')
        if temp_col.startswith('CBRAIN_'):
            styler = color_specific_value_cells(styler, study_tracking_df, temp_col, 'Satisfied', color='Green')
            styler = color_specific_value_cells(styler, study_tracking_df, temp_col, 'No File', color='Red')


    styler = color_specific_value_cells(styler, study_tracking_df, 'scans_tsv_present', 'True', color='Green')
    styler = color_specific_value_cells(styler, study_tracking_df, 'scans_tsv_present', 'False', color='Red')

    red_cbrain_statuses = ['Terminated', 'Failed To Setup', 'Failed To PostProcess', 'Failed Setup Prerequisites', 'Failed PostProcess Prerequisites', 'Suspended', 'Failed', 'Failed On Cluster']
    for temp_status in red_cbrain_statuses:
        styler = color_specific_value_cells(styler, study_tracking_df, 'CBRAIN_Status', temp_status, color='Red')
        styler = color_specific_value_cells(styler, study_tracking_df, 'CBRAIN_Status', temp_status, color='Red', secondary_column = 'subject')
    styler = color_specific_value_cells(styler, study_tracking_df, 'CBRAIN_Status', 'Completed', color='Green')
    styler = color_specific_value_cells(styler, study_tracking_df, 'derivatives_found', 'True', color='Green', secondary_column = 'subject')
    styler = color_specific_value_cells(styler, study_tracking_df, 'derivatives_found', 'True', color='Green', secondary_column = 'CBRAIN_Status')
    styler = color_specific_value_cells(styler, study_tracking_df, 'derivatives_found', 'True', color='Green')



    html = styler.to_html(index = False)
    title = "Summary for HBCD session {} {} processing".format(ses_label, pipeline_name)
    text_list = []
    text_list.append("The color coding for the subject label is a quick presentation of a subject's processing. Green indicates processing has been completed. Yellow indicates that processing wasn't attempted because some prerequisite file was missing. Red indicates failed attempts at processing.")
    text_list.append("It is important to remember that a failed processing task can be due to a variety of reasons including actual issues with how the pipeline is interacting with the imaging data, or data-agnostic issues within CBRAIN or MSI.")
    text_list.append('"Not Evaluated" entries occur when we identify soemthing in the processing routine that prevents processing from being attempted. This includes (1) previous attempts at processing, (2) missing scans.tsv file, (3) failure to have reqiured BIDS files of sufficient quality in one or more categories of imaging modalities, (4) missing prerequisite outputs from another pipeline.')
    text_list.append('Any columns beginning with "CBRAIN_" represent file collections in CBRAIN that are expected for the current processing pipeline. These columns generally represent the outputs from previous processing pipelines that will be fed to the current pipeline for processing. If a value of "No File" is observed, this either means processing with the other pipeline was not attempted or unsuccessful.')
    text_list.append('There are certain cases where a subject has specific file types that are missing or failing QC but processing still occurred for the subject. This is because there are multiple sets of criteria that are used to determine whether a subject should be processed. For example if a subject has a good T1w and T2w image then both will be included for processing, but processing will still occur if only a good T2w image is available for processing. For a subject to be processed, they must have all the files needed to satisfy at least one of the requirements files for the given pipeline defined at https://github.com/erikglee/HBCD_CBRAIN_PROCESSING/tree/qc_aware_proc/comprehensive_processing_prerequisites . If at least one requirement is satisfied then as many files as possible (up to the limits defined by the "num_to_keep" field) will be included in processing.')
    html = add_title_and_list_to_html_content(html, title, text_list)
    
    #Create dict with instructions for pie charts
    plot_dict = {}
    columns_to_skip_plotting = ['subject', 'pipeline', 'session']
    for temp_col in study_tracking_df.columns:
        if temp_col not in columns_to_skip_plotting:
            plot_dict[temp_col] = study_tracking_df[temp_col].value_counts().to_dict()
    
    #Add pie charts to html
    html = append_pie_charts_to_html(html, plot_dict)
    
    
    html = add_prettier_background_to_html(html)
    with open(output_html_path, 'w') as f:
        f.write(html)
        
    return study_tracking_df


def update_processing(pipeline_name, registered_and_s3_names, registered_and_s3_ids, cbrain_api_token,
                        group_id = '10367', user_id = '4022', bids_bucket_config = '/some/path',
                        bids_bucket = 'hbcd-pilot', bids_bucket_prefix = 'assembly_bids',
                        derivatives_bucket_config = '/some/path', derivatives_bucket = 'hbcd-pilot',
                        derivatives_bucket_prefix = 'derivatives', bids_data_provider_id = 710,
                        derivatives_data_provider_id = 0, session_qc_files_root_dir = None, ses_name = '',
                        rerun_level = 1, logs_directory = None, logs_prefix = 'cbrain_misc', session_dp_dict = None):
    
    '''Function to manage processing of data using CBRAIN

    Right now this function only supports processing in the case
    where the bids bucket and derivatives bucket are the same.
    In the future it will be desired to have functionality to
    support the case where the two buckets are different, such
    that a centralized BIDS bucket can be used by many investigators,
    and the outputs can be saved to a bucket that they own.
    
    This function will iterate through all BIDS subjects found in
    registered_and_s3_names and try to process them given the pipeline
    specified. (1) first checks if pipeline derivatives already exist in
    'bucket' for a given pipeline, only proceeding for subjects without results,
    (2) checks that all BIDS requirements are satisfied for that subject (as specified
    by jsons in processing_prerequisites), (3) grabs the required BIDS files for that
    subject using the specifications from processing_file_selection and processing_file_numbers,
    (4) creates an extended cbrain file list and uploads it to 'bucket' and registers in CBRAIN,
    (5) launches processing for the subject given the parameters specified from processing_configurations
    folder. 
    
    Parameters
    ----------
    pipeline_name : str
        Should be something like 'mriqc' or 'qsiprep' corresponding with
        pipelines specified in repository folder
    registered_and_s3_names: list of str
        The names of subjects registered in CBRAIN and found in S3 data provider
        that could potentially be processed
    registered_and_s3_ids: list of str
        Has the same ordering of registered_and_s3_names, and contains
        the CBRAIN ids for each subject
    cbrain_api_token: str
        The API token for your current CBRAIN session
    bids_data_provider_name: str, default 'HBCD-Pilot-Official'
        Name of DP as listed in CBRAIN
    user_name: str, default 'elee'
        The name of the CBRAIN user processing the data
    group_name: str, default 'HBCD-Computing'
        The name of the CBRAIN Group associated with the data to
        be processed
    bucket: str, default 'hbcd-pilot'
        The name of the bucket where the BIDS and derivatives data is stored
    rerun_level: 0, 1, 2, default 1
        If 0, then the script will not rerun any subjects that already have
        processing results in CBRAIN. If 1, then the script will rerun subjects
        that have processing results in CBRAIN but only if the processing results
        failed for CBRAIN/Cluster specific reasons. If 2, then the script will 
        also rerun processing in cases where processing failed on the cluster or
        in similar scenarios.
    logs_directory: str, default None
        If this path is set, some logs used to configure the CBRAIN processing
        for a given subject will be saved both to this directory and to the
        S3 bucket. If None, then no logs will be saved.
    
    '''

    if type(ses_name) == str:
        ses_label = ses_name.split('-')[1]

    #Load the tool config id for the current pipeline being used for processing
    tool_config_file = os.path.join(Path(inspect.getfile(update_processing)).absolute().parent.resolve(), 'tool_config_ids.json')
    with open(tool_config_file, 'r') as f:
        tool_config_dict = json.load(f)
    tool_config_id = str(tool_config_dict[pipeline_name])

    #Load the associated_files dictionary, which tells you which files
    #are associated with specific requirements (i.e. jsons for nii.gz, sbrefs, etc.)
    associated_files_file = os.path.join(Path(inspect.getfile(update_processing)).absolute().parent.resolve(), 'associated_files.json')
    with open(associated_files_file, 'r') as f:
        associated_files_dict = json.load(f)    
    
    #Load the "comprehensive_processing_prerequisites" json files that are the same for each subject
    #These files at least say which files are required for processing, and may also specify other
    #requirements such as the number of files required for processing or whether any QC requirements
    #need to be met for a file to be included in processing
    #requirements_files = glob.glob(os.path.join(Path(inspect.getfile(update_processing)).absolute().parent.resolve(), 'comprehensive_processing_prerequisites','{}*.json'.format(pipeline_name)))
    requirements_files = []
    comp_proc_recs_dir = os.path.join(Path(inspect.getfile(update_processing)).absolute().parent.resolve(), 'comprehensive_processing_prerequisites')
    for filename in os.listdir(comp_proc_recs_dir):
        # Use regular expression to match pipeline name with or without underscore and number
        match = re.match(rf"^{pipeline_name}(?:_[0-9]+)?\.json$", filename)
        if match:
            requirements_files.append(os.path.join(comp_proc_recs_dir, filename))

    requirements_dicts = []
    for temp_requirement_file in requirements_files:
        with open(temp_requirement_file, 'r') as f:
            requirements_dicts.append(json.load(f))
    print('{} requirements dictionaries: {}'.format(pipeline_name, requirements_dicts))

    #Rearange the requirements files dictionaries into one dictionary
    #that has all the possible file types that we will want to grab.
    file_selection_dict = {}
    for temp_dict in requirements_dicts:
        for temp_key in temp_dict.keys():
            if temp_key in file_selection_dict.keys():
                if temp_dict[temp_key] != file_selection_dict[temp_key]:
                    raise ValueError('Error: {} has conflicting requirements for {}'.format(pipeline_name, temp_key))
            else:
                file_selection_dict[temp_key] = temp_dict[temp_key]
    
    #If any of the requirements are dependent on QC info, return True
    #otherwise return false and allow processing even when QC file is missing
    qc_info_required = is_qc_info_required(file_selection_dict)
    print("QC info required: {}".format(qc_info_required))

    #Path to external requirements file for the given pipeline      
    external_requirements_file_path = os.path.join(Path(inspect.getfile(update_processing)).absolute().parent.resolve(), 'external_requirements', '{}.json'.format(pipeline_name))
    with open(external_requirements_file_path, 'r') as f:
        external_requirements_dict = json.load(f)
    print('{} external requirements dictionary: {}'.format(pipeline_name, external_requirements_dict))
    print('{} file selection dictionary: {}\n\n'.format(pipeline_name, file_selection_dict))
    ##################################################################
        
        
    #Code to be sure that only one BidsSubject entry is in the configuration json...
    #otherwise this script doesn't have logic to fullfill processing requirements...
    num_bids_subject_requirements = 0
    for temp_requirement in external_requirements_dict.keys():
        if external_requirements_dict[temp_requirement] == 'BidsSubject':
            num_bids_subject_requirements += 1
    if num_bids_subject_requirements > 1:
        raise ValueError('Error: This script was not designed to work for pipelines that take more than one BidsSubject inputs since the BidsSubject input will be replaced with an extended file list, and the script only knows how to generate one of these at a time.')
    ###################################################################################
        
    #Grab some CBRAIN info that will be referenced for all subjects being processed ########
    current_cbrain_tasks = find_current_cbrain_tasks(cbrain_api_token, data_provider_id = derivatives_data_provider_id) #this is bids_data_provider_id because we currently only support processing that grabs/saves from one DP
    bids_data_provider_files = find_cbrain_files_on_dp(cbrain_api_token, data_provider_id = bids_data_provider_id)
    derivs_data_provider_files = find_cbrain_files_on_dp(cbrain_api_token, data_provider_id = derivatives_data_provider_id)
    ########################################################################################
    
    #A list to store details about why some subjects were processed
    #and others were not
    study_processing_details = []
    
    subject_external_requirements_list = []
    all_to_keep_lists = [] #list of lists of files to keep for each subject
    final_subjects_ids_for_proc = [] #list of subjects that fullfill requirements for processing
    final_subjects_names_for_proc = [] #list of subjects that fullfill requirements for processing
    metadata_dicts_list = [] #list for keeping track of s3 file identifiers
    for i, temp_subject in enumerate(registered_and_s3_names):
        print('Evaluating: {}'.format(temp_subject))
        
        #A dictionary to store details that will later be used to populate
        #a spreadsheet that will be used to track processing across subjects
        if 'subject_processing_details' in locals():
            study_processing_details.append(subject_processing_details)


        subject_processing_details = {}
        subject_processing_details['subject'] = temp_subject
        subject_processing_details['pipeline'] = pipeline_name
        subject_processing_details['session'] = ses_name
        subject_processing_details['derivatives_found'] = "Not Evaluated"
        subject_processing_details['CBRAIN_Status'] = "Not Evaluated"
        subject_processing_details['scans_tsv_present'] = "Not Evaluated"
        
        #Be sure that the current subject doesn't have existing output before starting processing
        if check_if_derivatives_exist(temp_subject, pipeline_name,
                                        bucket = derivatives_bucket, prefix = derivatives_bucket_prefix,
                                        derivatives_bucket_config = derivatives_bucket_config):
            subject_processing_details['derivatives_found'] = True
            for temp_req in file_selection_dict.keys():
                subject_processing_details[temp_req] = 'Already Processed'
            for temp_req in external_requirements_dict.keys():
                subject_processing_details['CBRAIN_' + temp_req] = 'Already Processed'
            subject_processing_details['CBRAIN_Status'] = 'Already Processed'
            subject_processing_details['scans_tsv_present'] = "Already Processed"

            print('    Already has derivatives')
            continue
        else:
            subject_processing_details['derivatives_found'] = False

        #Check what type of processing has already occured for the subject with
        #this pipeline and only continue if processing hasn't already been initiated
        #or under certain failure conditions (see documentation for check_rerun_status)
        to_rerun, example_status = check_rerun_status(registered_and_s3_ids[i], current_cbrain_tasks, derivatives_data_provider_id, tool_config_id, rerun_level = rerun_level)
        subject_processing_details['CBRAIN_Status'] = example_status
        if False == to_rerun:
            continue #Check rerun status will print out a message to the user if processing is not going to be rerun

        #Grab the QC file for this subject so we can figure out which files can be used for processing.
        #If no QC requirements are specified in the comprehensive processing prerequisites, then the QC file will be ignored.
        if type(session_qc_files_root_dir) != type(None):
            subj_ses_qc_file_path = download_scans_tsv_file(bids_bucket_config, logs_directory, temp_subject, ses_name, bids_prefix = 'assembly_bids', bucket = bids_bucket, client = None)
            if (type(subj_ses_qc_file_path) == type(None)) and (qc_info_required == True):
                print('    Skipping Processing - No QC file found for subject')
                subject_processing_details['scans_tsv_present'] = False
                for temp_req in file_selection_dict.keys():
                    subject_processing_details[temp_req] = 'No scans.tsv'
                for temp_req in external_requirements_dict.keys():
                    subject_processing_details['CBRAIN_' + temp_req] = 'No scans.tsv'
                subject_processing_details['CBRAIN_Status'] = 'No scans.tsv'
                subject_processing_details['derivatives_found'] = "No (Missing scans.tsv)"
                continue
            else:
                subject_processing_details['scans_tsv_present'] = True
                if qc_info_required == False:
                    subj_ses_qc_file = None
                else:
                    subj_ses_qc_file = pd.read_csv(subj_ses_qc_file_path, delimiter = '\t')
        else:
            subj_ses_qc_file = None


        #First run preliminary check for requirements that is only used
        #for the purpose of populating the processing details spreadsheet.
        #Later requirements check will actually be used to determine if 
        #processing will be attempted.
        temp_req_output, req_tracking_dict = check_bids_requirements(temp_subject, file_selection_dict,
                                                    bucket = bids_bucket, prefix = bids_bucket_prefix,
                                                    bids_bucket_config = bids_bucket_config,
                                                    session = ses_name, qc_df = subj_ses_qc_file)
        subject_processing_details.update(req_tracking_dict)
            
        #Check that the subject has requirements satisfiying at least one pipeline specific json in the processing_prerequisites folder
        requirements_satisfied = 0
        none_found = 0
        for temp_requirement in requirements_dicts:
            temp_req_output, _ = check_bids_requirements(temp_subject, temp_requirement,
                                                                bucket = bids_bucket, prefix = bids_bucket_prefix,
                                                                bids_bucket_config = bids_bucket_config,
                                                                session = ses_name, qc_df = subj_ses_qc_file)
            #If return == None, this is because some QC info was expected but is missing
            if type(temp_req_output) == type(None):
                none_found = 1
            #Otherwise, a requirement was either passed or failed as expected
            else:
                requirements_satisfied += int(temp_req_output)
            
        if (requirements_satisfied == 0) or (none_found == 1):
            print('    Requirements not satisfied')
            subject_processing_details['derivatives_found'] = "No (Missing BIDS Reqs)"
            subject_processing_details['CBRAIN_Status'] = "No Proc. (Missing BIDS Reqs)"
            continue
            
        #Check that the external requirements are satisfied for the subject (these are pipeline inputs that will be files/file collections
        #that should already be available for the subject on CBRAIN if the subject is ready for processing). Note that
        #the files being passed to this function are already specific to a single data provider.
        subject_external_requirements, req_tracking_dict = grab_external_requirements(temp_subject, bids_data_provider_files + derivs_data_provider_files,
                                                                    external_requirements_dict, bids_data_provider_id = bids_data_provider_id,
                                                                    derivatives_data_provider_id = derivatives_data_provider_id) #implement function for this...
        
        #Update tracking dict based on grab_external_requirements results
        for temp_key in req_tracking_dict.keys():
            subject_processing_details['CBRAIN_' + temp_key] = req_tracking_dict[temp_key]
        
        #Skip processing for subject if external requirements aren't found
        if subject_external_requirements is None:
            print('    Missing external requirements')
            subject_processing_details['derivatives_found'] = "No (Missing Derived Reqs)"
            subject_processing_details['CBRAIN_Status'] = "No Proc. (Missing Derived Reqs)"
            continue #skip processing if external requirements aren't found
            
        #Grab files for the subject according to pipeline specific jsons in processing_file_numbers and processing_file_selection folders
        subject_files_list, metadata_dict = grab_required_bids_files(temp_subject, file_selection_dict, qc_df = subj_ses_qc_file,
                                                                 bucket = bids_bucket, prefix = bids_bucket_prefix,
                                                                 bids_bucket_config = bids_bucket_config,
                                                                 session = ses_name, associated_files_dict = associated_files_dict)

        #Save subject information with other subjects that are ready for processing
        all_to_keep_lists.append(subject_files_list)
        metadata_dicts_list.append(metadata_dict)
        subject_external_requirements_list.append(subject_external_requirements)
        final_subjects_ids_for_proc.append(registered_and_s3_ids[i])
        final_subjects_names_for_proc.append(temp_subject)
        print('    Processing will be attempted')
        subject_processing_details['CBRAIN_Status'] = 'Initiating Processing'

        #print('all_to_keep {}'.format(all_to_keep_lists))
        #print('subject_external_req {}'.format(subject_external_requirements_list))
        #print('subject_ids {}'.format(final_subjects_ids_for_proc))
        #print('subject_names {}'.format(final_subjects_names_for_proc))

    #Update the study processing details dict for the last subject
    if 'subject_processing_details' in locals():
        study_processing_details.append(subject_processing_details)
    
       
    #################################################################################################
    #################################################################################################
    #Iterate through all subjects who were deemed ready for processing,
    # and submit task to process their data in CBRAIN.
    print('\n\n')
    for i, temp_subject in enumerate(final_subjects_ids_for_proc):  
        print('Processing {} with {} via API'.format(final_subjects_names_for_proc[i], pipeline_name))

        #Run "mark as newer" to be sure the latest version of the subject data
        #is in the local CBRAIN cache once processing begins
        for temp_key in subject_external_requirements_list[i].keys():
            cbrain_mark_as_newer(subject_external_requirements_list[i][temp_key], cbrain_api_token)                

        #Launch Processing
        status, json_for_logging = launch_task_concise_dict(pipeline_name, subject_external_requirements_list[i], cbrain_api_token, data_provider_id = derivatives_data_provider_id,
                                    group_id = group_id, user_id = user_id, task_description = ' via API',
                                    all_to_keep = all_to_keep_lists[i], session_label = ses_label)
        json_for_logging['s3_metadata'] = metadata_dicts_list[i]
        if status == False:
            raise ValueError('Error CBRAIN processing tasked was not submitted for {}. Issue must be resolved for processing to continue.'.format(final_subjects_names_for_proc[i]))
        else:
            if type(logs_directory) != type(None):
                log_file_name = os.path.join(logs_directory, '{}_{}_UMNProcSubmission.json'.format(final_subjects_names_for_proc[i], pipeline_name))
                with open(log_file_name, 'w') as f:
                    json.dump(json_for_logging, f, indent = 4)
                #derivatives_bucket_prefix
                upload_processing_config_log(log_file_name, bucket = derivatives_bucket, prefix = os.path.join(derivatives_bucket_prefix, logs_prefix), bucket_config = derivatives_bucket_config)

    
    study_tracking_df = pd.DataFrame.from_dict(study_processing_details)
    #study_tracking_df.fillna('Not Evaluated', inplace=True)
    if type(logs_directory) != type(None):
        log_csv_name = os.path.join(logs_directory, 'processing_details_{}_{}.csv'.format(pipeline_name, ses_label))
        log_html_name = os.path.join(logs_directory, 'processing_details_{}_{}.html'.format(pipeline_name, ses_label))
        study_tracking_df = reformat_df_and_produce_proc_html(study_tracking_df, pipeline_name, ses_label, log_html_name, file_selection_dict)
        study_tracking_df.to_csv(log_csv_name, index = False)

    
    return study_tracking_df