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
import html_tools
import time


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


def find_potential_subjects_for_processing_v2(bids_data_provider_files, bids_bucket_config, bids_bucket = 'hbcd-pilot',
                                           bids_prefix = 'assembly_bids'):
    """Find subjects that may be ready for processing
    
    Looks for subjects that are already registered in CBRAIN
    and also exist in the S3 bucket. Returns arguments that
    will be used with update_fully_generic_processing_using_csv_dict
    to run processing with different pipelines.
    
    Parameters
    ----------

    bids_data_provider_files : list of dicts
        CBRAIN files on the BIDS DateProvider
    bids_bucket_config : str
        Path to the bids bucket config file
    bids_bucket : str
        Name of the S3 bucket to look for subjects in
    bids_prefix : str
        Prefix to look for subjects in (i.e. assembly_bids)
    
    Returns
    -------

    registered_and_s3 : list
        Subjects that are registered in CBRAIN and in S3
    registered_and_s3_ids : list
        CBRAIN IDs for the subjects that are registered in CBRAIN and in S3

    """

    #Find S3 Subjects
    s3_subjects = find_s3_subjects(bids_bucket_config, bucket = bids_bucket, prefix = bids_prefix)

    #Narrow down BIDS DP Files to BidsSubject instances
    cbrain_bids_subject_files = list(filter(lambda f: 'BidsSubject' == f['type'], bids_data_provider_files))

    #Find S3 subjects that are also registered in CBRAIN
    registered_and_s3_ids = []
    registered_and_s3_names = []
    for temp_subject in s3_subjects:
        for i, temp_cbrain in enumerate(cbrain_bids_subject_files):
            if temp_cbrain['name'] == temp_subject:
                registered_and_s3_ids.append(temp_cbrain['id'])
                registered_and_s3_names.append(temp_cbrain['name'])
                break

    return registered_and_s3_names, registered_and_s3_ids

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


def grab_cbrain_initialization_details(cbrain_api_token, group_name, bids_data_provider_name, session_data_providers):
    
    cbrain_groups = find_cbrain_entities(cbrain_api_token, 'groups')
    group_id = list(filter(lambda f: group_name == f['name'], cbrain_groups))
    if len(group_id) != 1:
        raise NameError('Error: Expected to find one CBRAIN Group ID corresponding with {} but found {}'.format(group_name, len(group_id)))
    else:
        group_id = group_id[0]['id']


    cbrain_dps = find_cbrain_entities(cbrain_api_token, 'data_providers')
    bids_data_provider_id = list(filter(lambda f: bids_data_provider_name == f['name'], cbrain_dps))
    if len(bids_data_provider_id) != 1:
        raise NameError('Error: Expected to find one CBRAIN Data Provider ID corresponding with {} but found {}'.format(bids_data_provider_name, len(bids_data_provider_id)))
    else:
        bids_bucket = bids_data_provider_id[0]['cloud_storage_client_bucket_name']
        bids_dp_id = bids_data_provider_id[0]['id']

    session_dps_dict = {}
    for temp_session_data_provider in session_data_providers:
        temp_data_provider_id = list(filter(lambda f: temp_session_data_provider == f['name'], cbrain_dps))
        if len(temp_data_provider_id) != 1:
            raise NameError('Error: Expected to find one CBRAIN Data Provider ID corresponding with {} but found {}'.format(temp_session_data_provider, len(temp_data_provider_id)))
        else:
            session_dps_dict[temp_session_data_provider] = {'id' : temp_data_provider_id[0]['id'],
                                                                       'bucket' : temp_data_provider_id[0]['cloud_storage_client_bucket_name'],
                                                                       'prefix' : temp_data_provider_id[0]['cloud_storage_client_path_start']}
    
    
    return group_id, bids_bucket, bids_dp_id, session_dps_dict


def find_cbrain_entities(cbrain_api_token, entity_type):

    base_url = 'https://portal.cbrain.mcgill.ca'
    tasks = []
    tasks_request = {'cbrain_api_token': cbrain_api_token, 'page': 1, 'per_page': 1000}

    while True:
        tasks_response = requests.get(
            url = '/'.join([base_url, entity_type]),
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
            
    return tasks

def grab_subject_file_info(subject_id, bids_bucket_config, bucket = 'hbcd-pilot', prefix = 'assembly_bids'):
    '''Utility that grabs BIDS data for a given subject
        
    Parameters
    ----------
    
    subject_id : str
        Subject ID such as 'sub-1234'
    bids_bucket_config: str
        The config for the S3 account used
        to access the bucket
    bucket: str, default 'hbcd-pilot'
        Name of bucket to query
    prefix: str, default 'assembly_bids'
        Where to start search (i.e. subfolder)
        within the bucket
        
    Returns
    -------
    
    List of dictionaries, one dictionary per
    object that a subject has in S3 BIDS
        
    '''


    
    # Do some cosmetics we look for the subject ID at the
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
                subject_files.append(temp_dict)
    return subject_files


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


def grab_session_specific_file_info(all_subject_files, session,
                                session_agnostic_files = ['sessions.tsv'],
                                session_level = None):
    '''Function that reduces list of S3 files to those from specific subject session
    
    Parameters
    ----------
    all_subject_files : list of dicts
        List of file object dictionaries from S3 for files
        that belong to a given subject
    session: str
        The session (i.e. 'ses-V02') that output files will be
        required to be associated with
    session_agnostic_files: list of strs, default ['sessions.tsv']
        Naming templates to accept for files that you want to
        include that don't specifically belong to a given session
    session_level: None or int, default None
        If None, the function will search for files that have the provided
        session info in the file name. If an integer such as 2 is provided,
        it will assume that there is organization such as /study/subject/ses/...
        and it will only accept files where the ses is found at that specific
        location (2 is like 3 here because of zero indexing)
    
    '''
    

    if type(session_level) == type(None):
        if session[0] != '/':
            session = '/' + session
        if session[-1] != '/':
            session = session + '/'

    session_files = []
    for temp_file in all_subject_files:
        if type(session_level) == type(None):
            if session in temp_file['Key']:
                session_files.append(temp_file)
        else:
            file_split = temp_file['Key'].split('/')[session_level]
            if file_split == session:
                session_files.append(temp_file)
        for temp_agnostic in session_agnostic_files:
            if temp_agnostic in temp_file['Key']:
                session_files.append(temp_file)

    return session_files
    

def is_qc_info_required(requirement_dictionary):
    '''Return True if QC info is required for any given requirement'''
    
    for temp_req in requirement_dictionary.keys():
        if 'qc_criteria' in requirement_dictionary[temp_req]:
            return True
    
    return False


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
        
def file_exists_under_prefix(bucket_name, prefix, s3_config):
    s3 = create_boto3_client(s3_config = s3_config)
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
    return 'Contents' in response
    
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
                print('    Requirement {} not found for subject {}'.format(temp_requirement, subject_name))
                return None, requirements_tracking_dict

    return subject_external_requirements, requirements_tracking_dict

def find_potential_subjects_for_processing(cbrain_api_token, bids_bucket_config, bids_bucket = 'hbcd-pilot',
                                           bids_prefix = 'assembly_bids', data_provider_id = '710',
                                           verbose = False):
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
    if verbose:
        print('Not Registered:')
        for temp_subject in list(non_registered_subjects):
            print('    {}'.format(temp_subject))
        print('Not in S3:')
        for temp_subject in list(missing_in_s3_subjects):
            print('    {}'.format(temp_subject))


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



def check_bids_requirements_v2(subject_id, session_files, requirements_dict,
                               qc_df = None, bucket = 'hbcd-pilot', prefix = 'assembly_bids',
                               bids_bucket_config = False, session = None,
                               session_agnostic_files = ['sessions.tsv'],
                               verbose = False):
    
    '''Function that checks if a subject has required BIDS data for processing
    
    See the full documentation for grab_required_bids_files_v2 for more context.
    This function is generally run before grab_required_bids_files_v2 to figure
    out whether the subject has all the BIDS files required for processing. Generally
    the requirements_dict given to the current function may be smaller than the 
    requirements_dict given to grab_required_bids_files_v2, and this function may
    be called several times to test different file requirement combinations, any
    one of which indicates that a subject can be processed. As long as one or more
    requirements_dict is given to the current function is satisfied then all
    requirements_dict objects are generally combined to allow for the union of
    all requirements to be present during processing. For example one requirement
    may be for a T1w image, another for a T2w image, this function may be called
    first to test the T1w image requirement then again to test the T2w image requirement
    but when grab_required_bids_files_v2 is called a joint requirements_dict might be
    used that says to grab all T1w images and T2w images if they are available and have
    the right quality. Of course if you only want to process subjects that have good
    T1w and good T2w images then you would give both this function and grab_required_bids_files_v2
    requirements_dict files that specify both image types.
    
    
    Parameters
    ----------
    subject_id : str
        Subject whose BIDS requirements are being checked.
        (i.e. sub-01)
    session_files : list of dicts
        A list of file info generated from boto3 that contains
        all files for a subject that are associated with the
        current session (i.e. ses-V02). This often also includes
        session agnostic files like the sessions.tsv file that
        some pipelines require for processing. See grab_subject_file_info
        and grab_session_specific_file_info for more details.
    requirements_dict : dict
        Dictionary listing possible requirements that must be
        satisfied for a file to be included in processing.
        (See example above)
    qc_df : None or pandas dataframe
        The scans.tsv file for the current session loaded
        as a pandas dataframe. This will be used for any
        inforporation of qc information.
    bucket : str, default 'hbcd-pilot'
        The name of the bucket to query for files
    prefix : str, default 'assembly_bids'
        The prefix to restrict the files returned by
    bids_bucket_config : bool or str, default False
        If bids_bucket_config is a string, this
        will be used as a config file to identify
        the s3 credentials... otherwise the default
        location will be used
    session : str
        The session being used for processing (i.e. ses-V02)
    session_agnostic_files : list of str, default ['sessions.tsv']
        List of files from session_files that are not actually session
        specific
    verbose : bool, default False
        Print more details

    
    Returns
    -------
    list
        List of file paths that satisfy the requirements
        and that will be used for processing.
    dict
        Dictionary that contains status information for
        the various high-level requirements represented
        within requirements_dict
        
    Returns
    -------
    
    bool
        True if requirements are all satisfied,
        otherwise False
    dict
        A dictionary that is used to help keep track
        of whether different processing requirements
        are satisfied for the current subject
        
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
        
        #Check if the current requirement has any associated QC criteria
        if verbose:
            print('Parent Requirement: {}'.format(parent_requirement))
        if 'qc_criteria' in requirements_dict[parent_requirement]:
            num_iters_allowed = len(requirements_dict[parent_requirement]['qc_criteria'])
            qc_index = 0
        else:
            num_iters_allowed = 1
            qc_index = None
            
        #The following loop to find files is necessary because there may
        #be cases where there is different QC information available for
        #different subjects, and we want to process as many subjects as
        #possible using manual QC measures when they are available, but
        #in absence of manual QC measures we may use automated QC measures.
        continue_loop = True
        while continue_loop:
            requirement_status, temp_tracking_str = check_bids_requirements_v2_inner(session_files, requirements_dict[parent_requirement], 
                                             qc_index = qc_index, qc_df = qc_df, 
                                             session_agnostic_files = session_agnostic_files, verbose = verbose)
            if verbose:
                print('Requirement Status {}: {}'.format(parent_requirement, requirement_status))
                print('Temp_tracking_str: {}'.format(temp_tracking_str))
            if type(qc_index) == type(None):
                continue_loop = False
            else:
                qc_index += 1
            if (type(requirement_status) == type(None)) and (qc_index != num_iters_allowed):
                continue
            else:
                continue_loop = False
            continue_loop = False
           
                             
        #print('Requirement Disqualified Status: {}\n'.format(requirement_disqualified))
        if requirement_status == True:
            parent_requirements_satisfied += 1
            temp_tracking_str = 'Satisfied'
        requirements_tracking_dict[parent_requirement] = temp_tracking_str
        
    if verbose:
        print('Num parent reqs satisfied: {}/{}'.format(parent_requirements_satisfied, len(requirements_dict.keys())))
    
    #If all requirements have been satisfied at least once return true, else false
    if parent_requirements_satisfied == len(requirements_dict.keys()):
        #print(len(requirements_dict.keys()))
        return True, requirements_tracking_dict
    else:
        return False, requirements_tracking_dict
    

def check_bids_requirements_v2_inner(session_files, partial_requirements_dict, qc_index = None, qc_df = None, 
                                         session_agnostic_files = ['sessions.tsv'], verbose = False):
    #This function looks for session files that have the right name for processing
    #and pass the QC Criteria (if there are any available). The reason why this
    #is a standalone function is so that different qc_indices can be used, signifying
    #different qc criteria that will be referred to based on what qc measures are
    #available for a given subject.
    
    
    #Grab the current QC criteria. Only necessary if QC Criteria
    #is in the partial_requirements_dict. If there are more than
    #one QC Criteria (i.e. there are backup QC criteria), then 
    #the qc_index will say which one is currently being referenced
    any_passing = False
    temp_tracking_status = 'No File'
    if verbose:
        print('Current QC Index: {}'.format(qc_index))
    if 'qc_criteria' in partial_requirements_dict:
        if type(qc_index) == type(None):
            temp_qc_criteria_group = partial_requirements_dict['qc_criteria']
        else:
            temp_qc_criteria_group = partial_requirements_dict['qc_criteria'][qc_index]
    
    temp_requirement_file_list = []
    child_requirements = partial_requirements_dict['file_naming']
    for j, temp_file in enumerate(session_files):
        child_requirements_satisfied = 0
        for child_requirement in child_requirements.keys():
            if (child_requirement in temp_file['Key']) == child_requirements[child_requirement]:
                child_requirements_satisfied += 1
            else:
                break
        if child_requirements_satisfied == len(child_requirements.keys()):
            temp_requirement_file_list.append(temp_file['Key'])


    for j, temp_file in enumerate(temp_requirement_file_list):
        requirement_disqualified = 0
        if ('qc_criteria' in partial_requirements_dict) and (type(qc_df) != type(None)):
            partial_df = qc_df[qc_df['filename'].str.contains(temp_file.split('/')[-1])]
            if len(partial_df) == 0:
                is_ses_agnostic = 0
                for temp_ses_agnostic in session_agnostic_files:
                    if temp_ses_agnostic in temp_file:
                        is_ses_agnostic = 1
                if is_ses_agnostic == 0:
                    print('   Exiting processing attempt: No QC info for {}'.format(temp_file))
                    return None, 'No QC'


            #Iterate through each QC requirement (the requirements are
            #stored as a list of dictionaries, each with one key/value pair)
            if verbose:
                print('File being investigated: {}'.format(temp_file.split('/')[-1]))
            if verbose:
                print('   temp_qc_criteria_group: {}'.format(temp_qc_criteria_group))
            for temp_qc_criteria in temp_qc_criteria_group:
                if verbose:
                    print('   temp_qc_criteria: {}'.format(temp_qc_criteria))
                for temp_dict_key in temp_qc_criteria:
                    
                    #Check if there is a null value for the current QC criteria.
                    #If there is a null value here, we will want to error out so
                    #the next grouping of QC criteria can be used if one is available.
                    #print('temp_dict: {}'.format(temp_dict))
                    #temp_dict_key = list(temp_dict.keys())[0]
                    if pd.isnull(partial_df[temp_dict_key].values[0]):
                        if verbose:
                            print('   SKIPPING CRITERIA BECAUSE NULL WAS IDENTIFIED: {}'.format(partial_df[temp_dict_key].values[0]))
                        temp_tracking_status = 'Missing QC'
                        return None, temp_tracking_status
                    
                    if verbose:
                        print('     is {} {} {} : {}'.format(partial_df[temp_dict_key].values[0], temp_qc_criteria[temp_dict_key][1], temp_qc_criteria[temp_dict_key][0], temp_file))
                    if make_comparison(partial_df[temp_dict_key].values[0], temp_qc_criteria[temp_dict_key][1], temp_qc_criteria[temp_dict_key][0]):
                        continue
                        #temp_tracking_status = 'Satisfied'
                    else:
                        if temp_tracking_status != 'Satisfied':
                            temp_tracking_status = 'Failed QC'
                        requirement_disqualified = 1
                    

        if requirement_disqualified == 0:
            any_passing = True
            temp_tracking_status = 'Satisfied'
            
    #If it gets to this point, it means that the current grouping
    #of QC criteria had all the requisite info to determine whether
    #the current file was good enough for processing. Then a true/false
    #rating mentions whether that criteria was satisfied or not.
    return any_passing, temp_tracking_status


def make_comparison(new_val, operator, reference):
    if operator == 'equals':
        return new_val == reference
    elif operator == 'less_than':
        return new_val < reference
    elif operator == 'greater_than':
        return new_val > reference
    else:
        raise NameError('Error: unknown operator {}'.format(operator))
    
def find_associated_files(subject_id, associated_files_dict, output_file_list,
                          session_files, prefix):
    '''
    Used by grab_required_bids_files_v2
    '''
            
    #Also find all the various files that are associated with the requirements
    metadata_dict = {}
    
    if type(associated_files_dict) != type(None):
        
        new_files = []
        for temp_file in output_file_list:
            for temp_key in associated_files_dict.keys():
                if temp_key in temp_file:
                    for temp_term in associated_files_dict[temp_key]:
                        
                        temp_file_path = temp_file.replace(temp_key, temp_term)
                        for temp_dict in session_files:
                            if temp_dict['Key'] == temp_file_path:
                                new_file_name = temp_dict['Key']
                                new_files.append(new_file_name)
                                metadata_dict[new_file_name] = temp_dict

        output_file_list = list(set(output_file_list + new_files))
        output_file_list.sort()
                
    return output_file_list, metadata_dict
    


def grab_required_bids_files_v2(subject_id, session_files, requirements_dict, qc_df = None, bucket = 'hbcd-pilot',
                                prefix = 'assembly_bids', bids_bucket_config = False, session = None, 
                                session_agnostic_files = ['sessions.tsv'], associated_files_dict = None,
                                verbose = False):
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
    
    One field of the requirements dictionary may be 'qc_criteria'.
    If 'qc_criteria' is a field, then it assumes that this field is
    a list of list of dictionaries. The length of the first list
    determines how many possible criteria can be used to figure
    out if a file should be used for processing and further which
    files should be used if processing. If all relevant files can
    be judged based on the first entry of this outer list, then
    that first list will be used to determine the QC criteria by
    which files will be judged. If one or more files has an undefined
    QC value (null/nan) that is needed to judge the quality of the file,
    then the second entry in the first list will be used to determine
    quality. Generally the first entry will contain a list of measures,
    some of which include manual QC that wont always be present, and
    the second list serves as a backup in the case that manual QC is
    missing.
    
    

    Example requirements_dict
    -------------------------

    The dictionary content below gives criteria for how to select T1 and
    T2 images. Because both fields have the "num_to_keep" field set to 1,
    up to 1 image will be included for each field in processing. The
    "file_naming" entry says which phrases should or shouldnt be included
    in the name of T1 and T2 images. The "qc_criteria" says how to judge
    an images quality. The keys in the innermost dictionaries under
    "qc_criteria" will correspond to column names in the subject's scans.tsv
    file. The following entry means that "HBCD_compliant" should be "Yes":
    
        "HBCD_compliant" : ["Yes", "equals"]
        
    The following entry means that "QU_motion" should be less than 2:
    
        {"QU_motion" : [2, "less_than"]}
        
    The following entry means that brain_SNR should be more than -1000:
    
        {"brain_SNR" : [-1000, "greater_than"]}
        
    In some cases criteria may be set to be extremely inclusive. For example
    the brain_SNR criteria will never say that an image should be excluded
    (because brain_SNR will always be greater than or equal to zero), but
    it will sometimes be used to decide which image is better when two or
    more images will be considered for processing. In the case of "T1" below,
    first "HBCD_compliant", then "QU_motion" then "aqc_motion", and then
    "brain_SNR" will be used to determine which image is better. If one or
    more of these measures are undefined for any of the images that match
    the "T1" naming criteria, then the script will attempt to use the second
    "qc_criteria" to judge files. In this case the two sets of criteria are
    the same, except for the second set does not have "QU_motion" which is
    a manually curated quality control measure that is often unavailable.
    
    {
    "T1" : {"file_naming" : {"T1w.nii.gz" : true,
            "rec-undistorted" : false,
            "acq-svslocalizer" : false,
            "acq-mrsLoc" : false,
            "_QALAS.nii.gz" : false},

            "qc_criteria" : [[{"HBCD_compliant" : ["Yes", "equals"]},
                                {"QU_motion" : [2, "less_than"]},
                                {"aqc_motion" : [10000, "less_than"]},
                                {"brain_SNR" : [-1000, "greater_than"]}],
                             [{"HBCD_compliant" : ["Yes", "equals"]},
                                {"aqc_motion" : [10000, "less_than"]},
                                {"brain_SNR" : [-1000, "greater_than"]}]],
            "num_to_keep" : 1
            },

    "T2" : {"file_naming" : {"T2w.nii.gz" : true,
                "rec-undistorted" : false,
                "acq-svslocalizer" : false,
                "acq-mrsLoc" : false,
                "_QALAS.nii.gz" : false},

                "qc_criteria" : [[{"HBCD_compliant" : ["Yes", "equals"]},
                                    {"QU_motion" : [2, "less_than"]},
                                    {"aqc_motion" : [10000, "less_than"]},
                                    {"brain_SNR" : [-1000, "greater_than"]}],
                                 [{"HBCD_compliant" : ["Yes", "equals"]},
                                    {"aqc_motion" : [10000, "less_than"]},
                                    {"brain_SNR" : [-1000, "greater_than"]}]],

                "num_to_keep" : 1
                },

    "sessions" : {"file_naming" : {"sessions.tsv" : true}}
    }
    
    Parameters
    ----------
    subject_id : str
        Subject whose BIDS requirements are being checked.
        (i.e. sub-01)
    session_files : list of dicts
        A list of file info generated from boto3 that contains
        all files for a subject that are associated with the
        current session (i.e. ses-V02). This often also includes
        session agnostic files like the sessions.tsv file that
        some pipelines require for processing. See grab_subject_file_info
        and grab_session_specific_file_info for more details.
    requirements_dict : dict
        Dictionary listing possible requirements that must be
        satisfied for a file to be included in processing.
        (See example above)
    qc_df : None or pandas dataframe
        The scans.tsv file for the current session loaded
        as a pandas dataframe. This will be used for any
        inforporation of qc information.
    bucket : str, default 'hbcd-pilot'
        The name of the bucket to query for files
    prefix : str, default 'assembly_bids'
        The prefix to restrict the files returned by
    bids_bucket_config : bool or str, default False
        If bids_bucket_config is a string, this
        will be used as a config file to identify
        the s3 credentials... otherwise the default
        location will be used
    session : str
        The session being used for processing (i.e. ses-V02)
    session_agnostic_files : list of str, default ['sessions.tsv']
        List of files from session_files that are not actually session
        specific
    associated_files_dict : None, or dict, default None
        A dictionary that lets you find misc. files that
        are associated with imaging files. For example
        json files, sbref files, bval, bvec files, etc.
    verbose : bool, default False
        Print more details

    
    Returns
    -------
    list
        List of file paths that satisfy the requirements
        and that will be used for processing.
    dict
        Dictionary that S3 information for the different
        files that have been selected for processing
        
    '''
    
    output_file_list = []
    metadata_dict = {}
    for i, parent_requirement in enumerate(requirements_dict.keys()):
        #Check if the current requirement has any associated QC criteria
        if verbose:
            print('Parent Requirement: {}'.format(parent_requirement))
        if 'qc_criteria' in requirements_dict[parent_requirement]:
            num_iters_allowed = len(requirements_dict[parent_requirement]['qc_criteria'])
            qc_index = 0
        else:
            num_iters_allowed = 1
            qc_index = None
            
        #The following loop to find files is necessary because there may
        #be cases where there is different QC information available for
        #different subjects, and we want to process as many subjects as
        #possible using manual QC measures when they are available, but
        #in absence of manual QC measures we may use automated QC measures.
        continue_loop = True
        while continue_loop:
            #if 1:
            try:
                partial_file_list, partial_metadata_dict = grab_required_bids_files_inner(session_files, requirements_dict[parent_requirement], 
                                                 qc_index = qc_index, qc_df = qc_df, 
                                                 session_agnostic_files = session_agnostic_files, verbose = verbose)
                continue_loop = False
            #if 1:
            except Exception as error:
                
                #I don't think this will ever occur
                if type(qc_index) == type(None):
                    raise ValueError('Error: No QC expected for current pipeline, but grab_bids_files_v2_inner still failed.')
                #If there are more QC Criteria that can be used
                #then try the next one (since previous one already failed)
                else:
                    qc_index += 1
                    if qc_index == num_iters_allowed:
                        print('   No QC list was properly evaluated for this subject:')
                        print(error)
                        continue_loop = False
           
        #This should update be only be made once per parent requirement
        #after the while loop finishes
        output_file_list = output_file_list + partial_file_list
        metadata_dict.update(partial_metadata_dict)
            

        
    #Also find all the various files that are associated with the requirements
    output_file_list, partial_metadata_dict = find_associated_files(subject_id, associated_files_dict, output_file_list,
                              session_files, prefix)
    metadata_dict.update(partial_metadata_dict)

    #Reformat the date information
    for a in metadata_dict.keys():
        for b in metadata_dict[a].keys():
             if isinstance(metadata_dict[a][b], datetime.datetime):
                metadata_dict[a][b] = metadata_dict[a][b].isoformat()
                
    # Do some cosmetics to change paths to be relative to subject level
    full_prefix = os.path.join(prefix, subject_id)
    prefix_offset = len(full_prefix.split('/'))
    metadata_dict_clean = {}
    for temp_key in metadata_dict.keys():
        metadata_dict_clean['/'.join(temp_key.split('/')[prefix_offset:])] = metadata_dict[temp_key]
    output_file_list_clean = []
    for temp_file in output_file_list:
        output_file_list_clean.append('/'.join(temp_file.split('/')[prefix_offset:]))
                
    return output_file_list_clean, metadata_dict_clean

def grab_required_bids_files_inner(session_files, partial_requirements_dict, qc_index = None, qc_df = None, 
                                         session_agnostic_files = ['sessions.tsv'], verbose = False):
    #This function looks for session files that have the right name for processing
    #and pass the QC Criteria (if there are any available). The reason why this
    #is a standalone function is so that different qc_indices can be used, signifying
    #different qc criteria that will be referred to based on what qc measures are
    #available for a given subject.
    
    
    #Grab the current QC criteria. Only necessary if QC Criteria
    #is in the partial_requirements_dict. If there are more than
    #one QC Criteria (i.e. there are backup QC criteria), then 
    #the qc_index will say which one is currently being referenced
    if verbose:
        print('Current QC Index: {}'.format(qc_index))
    if 'qc_criteria' in partial_requirements_dict:
        if type(qc_index) == type(None):
            temp_qc_criteria_group = partial_requirements_dict['qc_criteria']
        else:
            temp_qc_criteria_group = partial_requirements_dict['qc_criteria'][qc_index]
    
    temp_requirement_file_list = []
    temp_requirements_metadata_list = []
    child_requirements = partial_requirements_dict['file_naming']
    for j, temp_file in enumerate(session_files):
        child_requirements_satisfied = 0
        for child_requirement in child_requirements.keys():
            if (child_requirement in temp_file['Key']) == child_requirements[child_requirement]:
                child_requirements_satisfied += 1
            else:
                break
        if child_requirements_satisfied == len(child_requirements.keys()):
            temp_requirement_file_list.append(temp_file['Key'])
            temp_requirements_metadata_list.append(temp_file)

    counter = 0
    partial_output_file_list = []
    partial_output_file_list_qc = []
    partial_metadata_dict = {}
    for j, temp_file in enumerate(temp_requirement_file_list):
        qc_values = []
        requirement_disqualified = 0
        if ('qc_criteria' in partial_requirements_dict) and (type(qc_df) != type(None)):
            partial_df = qc_df[qc_df['filename'].str.contains(temp_file.split('/')[-1])]
            if len(partial_df) == 0:
                is_ses_agnostic = 0
                for temp_ses_agnostic in session_agnostic_files:
                    if temp_ses_agnostic in temp_file:
                        is_ses_agnostic = 1
                if is_ses_agnostic == 0:
                    print('   Exiting processing attempt: No QC info for {}'.format(temp_file))
                    return None, None


            #Iterate through each QC requirement (the requirements are
            #stored as a list of dictionaries, each with one key/value pair)
            if verbose:
                print('File being investigated: {}'.format(temp_file.split('/')[-1]))
            if verbose:
                print('   temp_qc_criteria_group: {}'.format(temp_qc_criteria_group))
            for temp_qc_criteria in temp_qc_criteria_group:
                if verbose:
                    print('   temp_qc_criteria: {}'.format(temp_qc_criteria))
                for temp_dict_key in temp_qc_criteria:
                    
                    #Check if there is a null value for the current QC criteria.
                    #If there is a null value here, we will want to error out so
                    #the next grouping of QC criteria can be used if one is available.
                    #print('temp_dict: {}'.format(temp_dict))
                    #temp_dict_key = list(temp_dict.keys())[0]
                    if pd.isnull(partial_df[temp_dict_key].values[0]):
                        if verbose:
                            print('   SKIPPING CRITERIA BECAUSE NULL WAS IDENTIFIED: {}'.format(partial_df[temp_dict_key].values[0]))
                        raise ValueError('    QC info not available for {}: {} ({})'.format(temp_dict_key, partial_df[temp_dict_key].values[0], temp_file))
                    
                    if verbose:
                        print('     is {} {} {} : {}'.format(partial_df[temp_dict_key].values[0], temp_qc_criteria[temp_dict_key][1], temp_qc_criteria[temp_dict_key][0], temp_file))
                    if make_comparison(partial_df[temp_dict_key].values[0], temp_qc_criteria[temp_dict_key][1], temp_qc_criteria[temp_dict_key][0]):
                        qc_values.append(partial_df[temp_dict_key].values[0])
                    else:
                        requirement_disqualified = 1
                    

        #I think the code below should work whether or not QC information is present
        if requirement_disqualified == 0:
            if 'num_to_keep' not in partial_requirements_dict.keys():
                partial_output_file_list.append(temp_file)
                partial_output_file_list_qc.append(qc_values)
                partial_metadata_dict[temp_file] = temp_requirements_metadata_list[j]
            elif partial_requirements_dict['num_to_keep'] > counter:
                partial_output_file_list.append(temp_file)
                partial_output_file_list_qc.append(qc_values)
                partial_metadata_dict[temp_file] = temp_requirements_metadata_list[j]
                counter += 1
            else:                
                
                if verbose:
                    print('Partial output file list qc: {}'.format(partial_output_file_list_qc))
                #Check which of the existing files has the worst QC values
                worst_existing_file_qc = partial_output_file_list_qc[0]
                worst_existing_file_qc_index = 0
                for qc_ind, temp_qc_partial in enumerate(partial_output_file_list_qc):
                    for temp_qc_item in range(len(partial_output_file_list_qc[0])):
                        if verbose:
                            print('temp_qc_partial {}'.format(temp_qc_partial))
                            print('qc_ind {}'.format(qc_ind))
                        #If QC measure is worse in the current file, then remark this as the worst file
                        #if temp_qc_partial[temp_qc_item] > worst_existing_file_qc[temp_qc_item]:
                        temp_inner_qc_criteria_dict = temp_qc_criteria_group[temp_qc_item]
                        temp_operator = list(temp_inner_qc_criteria_dict.values())[0][1]
                        if make_comparison(worst_existing_file_qc[temp_qc_item], temp_operator, temp_qc_partial[temp_qc_item]): #Ordering of make_comparison is swapped here to highlight the worst
                            #If QC measure is the same in the current file, check the next item
                            if temp_qc_partial[temp_qc_item] == worst_existing_file_qc[temp_qc_item]:
                                continue
                            #If QC measure is better in the current file, move on to the next file
                            else:
                                worst_existing_file_qc_index = qc_ind
                                worst_existing_file_qc = partial_output_file_list_qc[qc_ind]
                                break
                                
                if verbose:
                    print('   Worst existing file qc: {}'.format(worst_existing_file_qc))
                    print('   Worst existing file qc index: {}'.format(worst_existing_file_qc_index))

                #See if the current file is better/worse than the worst file
                for temp_qc_item in range(len(worst_existing_file_qc)):

                    #If new file is better, then update the file list with the
                    #current file
                    temp_inner_qc_criteria_dict = temp_qc_criteria_group[temp_qc_item]
                    temp_operator = list(temp_inner_qc_criteria_dict.values())[0][1]
                    #if worst_existing_file_qc[temp_qc_item] > qc_values[temp_qc_item]:
                    if verbose:
                        print('is {} {} {}'.format(qc_values[temp_qc_item], temp_operator, worst_existing_file_qc[temp_qc_item]))
                        
                    #Skip the comparison if the measure is boolean. If both files are considered, then they both will
                    #have already passed this.
                    if type(qc_values[temp_qc_item]) == bool:
                        continue
                    #If there is a tie, then check the next measure. Eventually this will lead to one file being
                    #arbritrarily being chosen based on file name if there are ties all the way down (this is fine)
                    elif qc_values[temp_qc_item] == worst_existing_file_qc[temp_qc_item]:
                        continue
                    elif make_comparison(qc_values[temp_qc_item], temp_operator, worst_existing_file_qc[temp_qc_item]):
                        del partial_metadata_dict[partial_output_file_list[worst_existing_file_qc_index]]
                        partial_metadata_dict[temp_file] = temp_requirements_metadata_list[j]
                        partial_output_file_list[worst_existing_file_qc_index] = temp_file
                        partial_output_file_list_qc[worst_existing_file_qc_index] = qc_values
                        break
                    #If new file is worse, then move on and ignore the current file
                    #elif worst_existing_file_qc[temp_qc_item] > qc_values[temp_qc_item]:
                    else:
                        break


    return partial_output_file_list, partial_metadata_dict

def check_all_files_old_enough(metadata_dict, minimum_file_age_days, 
                               file_patterns_to_ignore = ['sessions.tsv'],
                               verbose = False):
    '''
    If all files in metadata_dict have timestamp of at least minimum_file_age_days
    ago, return True, otherwise return False. Allow files to be excluded in this
    age comparison through the file_patterns_to_ignore list.
    '''
    
    
    today = date.today()
    
    for temp_file in metadata_dict.keys():
        skip_file = False
        for temp_pattern in file_patterns_to_ignore:
            if metadata_dict[temp_file]['Key'].endswith(temp_pattern):
                skip_file = True
                break
        
        if skip_file == False:
            file_upload_day = date.fromisoformat(metadata_dict[temp_file]['LastModified'].split('T')[0])
            day_difference = today - file_upload_day
            if verbose:
                print('{} Uploaded {} days ago'.format(temp_file.split('/')[-1], day_difference.days))
            if day_difference.days < minimum_file_age_days:
                return False
        
    return True

def load_requirements_infos(pipeline_name):

    #Load the "comprehensive_processing_prerequisites" json files that are the same for each subject
    #These files at least say which files are required for processing, and may also specify other
    #requirements such as the number of files required for processing or whether any QC requirements
    #need to be met for a file to be included in processing
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
    
    return requirements_dicts, file_selection_dict

def download_cbrain_misc_file(derivative_bucket_config, derivatives_bucket_prefix,
                              subject, bucket, pipeline_name, output_folder,
                              ending = 'UMNProcSubmission.json'):
    '''Download json from cbrain_misc folder
    
    Assumes there will be a file in the derivatives bucket with
    the naming convention:
    
    {bucket}/{derivatives_bucket_prefix}/cbrain_misc/{subject}_{pipeline_name}_{ending}
        
    Returns
    -------
    
    Path to downloaded json file if download was successful,
    otherwise returns None
        
    '''

    client = create_boto3_client(s3_config = derivative_bucket_config)
    file_to_download = os.path.join(derivatives_bucket_prefix, 'cbrain_misc', '{}_{}_{}'.format(subject, pipeline_name, ending))
    downloaded_file = os.path.join(output_folder, file_to_download.split('/')[-1])
    try:
        client.download_file(bucket, file_to_download, downloaded_file)
    except:
        return None
            
    return downloaded_file

def check_if_ancestor_file_selection_is_same(subject_id, session_files, ancestor_pipelines_file_selection_dict, qc_df = None,
                                             bids_bucket = None, bids_prefix = None, bids_bucket_config = None,
                                             session = None, session_agnostic_files = ['sessions.tsv'], associated_files_dict = None,
                                             verbose = False, derivatives_bucket_config = None, derivatives_bucket = None,
                                             derivatives_bucket_prefix = None, logs_directory = None):
    
    '''
    Function that assumes there will be a directory named "cbrain_misc"
    in the derivatives bucket, underneath the derivatives_bucket_prefix.
    The function then looks for log files that were created when a subject
    was previously processed with an "ancestor" pipeline. The function then
    loads the json log file to check if the files that would be selected
    for processing today (using current QC criteria) are the same as the
    files that were previously selected for processing. This includes checking
    the file names and file sizes based on s3 metadata. If there is overlap for
    all pipelines, the function returns true otherwise the function returns false.
    
    All inputs used for this pipeline are also used in various other functions
    in this file...
    
    '''
    
    
    for temp_pipeline in ancestor_pipelines_file_selection_dict.keys():
        temp_reqs = ancestor_pipelines_file_selection_dict[temp_pipeline]
        _, current_file_metadata = grab_required_bids_files_v2(subject_id, session_files, temp_reqs, qc_df = qc_df, bucket = bids_bucket,
                                                                                prefix = bids_prefix, bids_bucket_config = bids_bucket_config, session = session, 
                                                                                session_agnostic_files = session_agnostic_files, associated_files_dict = associated_files_dict,
                                                                                verbose = verbose)

        json_path = download_cbrain_misc_file(derivatives_bucket_config, derivatives_bucket_prefix,
                              subject_id, derivatives_bucket, temp_pipeline, logs_directory,
                              ending = 'UMNProcSubmission.json')
        
        if type(json_path) == type(None):
            print('   Warning: no ancestor cbrain_misc was identified. Assuming subject should not be processed.')
            return False
        else:
            with open(json_path, 'r') as f:
                json_content = json.load(f)
            os.remove(json_path)
            original_s3_metadata = json_content['s3_metadata']
            original_keys_sorted = list(original_s3_metadata.keys())
            original_keys_sorted.sort()
            current_keys_sorted = list(current_file_metadata.keys())
            current_keys_sorted.sort()
            if original_keys_sorted != current_keys_sorted:
                print('   Files that would be selected for {} processing today are different than what is found in the processing logs. Delete previous results and reprocess if you want to run the current pipeline.'.format(temp_pipeline))
                print('   Files that were previously used: {}'.format(original_keys_sorted))
                print('   Files that would be selected today: {}'.format(current_keys_sorted))
                
                return False
            else:
                for temp_file in original_s3_metadata.keys():
                    skip_file = False
                    for temp_agnostic in session_agnostic_files:
                        if temp_file.endswith(temp_agnostic):
                            skip_file = True
                            break #dont evaluate the size of session agnostic files
                    if skip_file == False:
                        if original_s3_metadata[temp_file]['Size'] != current_file_metadata[temp_file]['Size']:
                            print('Chosen file {} has different size ({} vs. now {}) when compared to when {} was ran.'.format(temp_file.split('/')[-1], original_s3_metadata[temp_file]['Size'], current_file_metadata[temp_file]['Size'], temp_pipeline))
                            return False
                        
        
    
    return True

def update_processing(pipeline_name = None,
                        cbrain_api_token = None,
                        session_data_provider_names = None,
                        group_name = None,
                        user_id = None,
                        bids_bucket_config = None,
                        bids_bucket_prefix = 'assembly_bids',
                        bids_data_provider_name = None,
                        derivatives_bucket_config = None,
                        logs_directory = None,
                        logs_prefix = 'cbrain_misc',
                        rerun_level = 1,
                        session_agnostic_files = ['sessions.tsv'],
                        check_ancestor_pipelines = True,
                        verbose = False,
                        minimum_file_age_days = 14,
                        max_subject_sessions_to_proc = None):
    
    '''Function to manage processing of data using CBRAIN
    
    This function will iterate through all BIDS subjects found in
    registered_and_s3_names and try to process them given the pipeline
    specified. The steps are generally as seen below.

    (Updated on 5/22/2024)

    The following procedure is used to first find which subjects we want
    to process:
    (1) - Check if pipeline derivatives exist in the derivatives bucket. If one or
    more files already exist, skip processing for the current subejct.
    (2) - Check if the subject has been processed in CBRAIN. The behavior of this
    step depends on the status of rerun_level. Based on the CBRAIN task status and
    rerun_level, the script will decide whether to process the subject or not.
    (3) - Optionally download the scans.tsv file for the subject/session combo being
    evaluated. This file can contain QC information that is used to determine if a
    subject should be processed. If the file is expected but not present, a subject
    won't be processed.
    (4) Grab a list of the subjects' BIDS files from the bids_bucket, and reduce this
    list to the files that are related to the current session (along with some session
    agnostic files like sessions.tsv).
    (5) Run an initial check of BIDS requirements that will be used to populate a HTML
    file that describes processing.
    (6) Check that at least one requirement file for the pipeline (found under comprehensive_processing_prerequisites)
    is satisfied for the subject. If no requirements are satisfied, then the subject will not be processed.
    (7) Check that all external requirements for the current pipeline are satisfied. If any are not
    satisfied, then the subject will not be processed.
    (8) Grab any BIDS files that satisfy at least one requirement in a comprehensive_processing_prerequisites
    file. Some requirements (i.e. T1) may be present in more than one requirements file, or other requirements
    may only be found in one file. As long as any processing requirement file is satisfied, the inidividual
    requirements from any file will be allowed. (Note: a requirement category is forced to have the same
    definition across files.)
    (9) Check that all files (except generally the sessions.tsv) are at least minimum_file_age_days old. If
    any file is too new, then the subject will not be processed.
    (10) Check that the file selection procedure for ancestor pipelines yields the same group of files 
    (judged by file name and size) as when the ancestor pipeline was originally ran. If this is not the
    case, skip processing for the current subject.
    
    The previous steps are used to compile all subjects that should be processed, along with the
    settings that should be used to process them. Once this process is complete:

    (11) iterate through all subjects to be processed and submit them to CBRAIN for processing. The
    only files that will be included for processing are files that are explicitly found in the previous
    steps. This means files from certain modalities may be excluded from the processing environment if
    they are not needed for processing.
    (12) Create csv and html files under the logs_directory that can be used to track processing progress.
    Be aware that if certain fields aren't filled out, this is generally because the subject has already
    been processed so fields from the later portion of this script are not filled out.

    (1) first checks if pipeline derivatives already exist in
    'bucket' for a given pipeline, only proceeding for subjects without results,
    (2) checks that all BIDS requirements are satisfied for that subject (as specified
    by jsons in comprehensive_processing_prerequisites), (3) grabs the required BIDS 
    files for that subject using the specifications from processing_file_selection and processing_file_numbers,
    (4) creates an extended cbrain file list and uploads it to 'bucket' and registers in CBRAIN,
    (5) launches processing for the subject given the parameters specified from processing_configurations
    folder. 

    Parameters
    ----------

    pipeline_name : str
        Should be something like 'mriqc' or 'qsiprep' corresponding with
        the folder where pipeline outputs would be stored in the derivatives
        bucket. This corresponds to the pipeline specific info for the primary
        output in the BoutiquesForcedOutputBrowsePath of the boutiques that
        is associated with processing.
    cbrain_api_token : str
        The API token for your current CBRAIN session. Needs to be refreshed
        periodically.
    session_data_provider_names : list of str
        The names of the CBRAIN DataProviders that are used for the session
        specific data.
    group_name : str
        The CBRAIN group name associated with processing.
    user_id : str,
        The CBRAIN user ID associated with processing. To
         find this, login to CBRAIN, go to "My Account",
          and look at the 4 or 5 digit numeric ID in the
           HTML address.
    bids_bucket_config : str, default '/some/path'
        The path to the s3 config file for the BIDS bucket
    bids_bucket_prefix : str, default 'assembly_bids'
        The folder(s) under the BIDS bucket where the top
        reference to the BIDS study-wide directory is found
    bids_data_provider_name : str
        The CBRAIN DataProvider Name for where the BIDS data is stored
    derivatives_bucket_config : str
        Same as BIDS bucket config, but for derivatives. This can either
        be the same or different as the BIDS bucket config.
    logs_directory : str or None, default None
        Working directory where scans.tsv files will be temporarily downloaded and
        also other logs describing subject processing before the logs are sent to
        S3. These files will also be deleted during processing. HTML and csv files 
        describing processing will also be stored here and will be kept after processing.
        If None is used, spooky behavior will be observed.
    logs_prefix : str, default 'cbrain_misc'
        The prefix to use when placing the logs file in the Bucket path associated
        with the Derivatives Data Provider.
    rerun_level : 0, 1, 2, default 1
        If 0, then the script will not rerun any subjects that already have
        processing results in CBRAIN. If 1, then the script will rerun subjects
        that have processing results in CBRAIN but only if the processing results
        failed for CBRAIN/Cluster specific reasons. If 2, then the script will 
        also rerun processing in cases where processing failed on the cluster or
        in similar scenarios.
    session_agnostic_files : list of str, default ['sessions.tsv']
        This will probably not be adjusted by users. This field is
        used internally at several points to make sure the sessions.tsv
        file is included in processing, and for other purposes also...
    check_ancestor_pipelines : bool, default True
        If True, check that file selection procedure for previously
        ran ancestor pipelines yields the same group of files as
        when the ancestor pipeline was originally ran.
    verbose : bool, default False
        Whether to print more text during processing
    minimum_file_age_days : int, default 14
        The minimum number of days that a file must be old before
        it can be used for processing. Set to 0 if you don't want
        this to be influencing processing routines.
    max_subject_sessions_to_proc : int, default None
        The maximum number of processing jobs to launch (where each
        session processed for a given subject is considered 1 proc).
        Default behavior is to process all subjects found in the BIDS
        Data Provider, or otherwise will only process the specified
        number. Subjects that fail to meet preprocessing requirements
        to not count to this total.

    Returns
    -------

    A pandas dataframe that describes processing.
    
    '''


    group_id, bids_bucket, bids_data_provider_id, session_dps_dict = grab_cbrain_initialization_details(cbrain_api_token,
                                                                                             group_name,
                                                                                             bids_data_provider_name,
                                                                                             session_data_provider_names)

    print("You are currently attempting to launch processing jobs with the tool {}.\n".format(pipeline_name))

    #Load the tool config id for the current pipeline being used for processing
    tool_config_file = os.path.join(Path(inspect.getfile(update_processing)).absolute().parent.resolve(), 'tool_config_ids.json')
    with open(tool_config_file, 'r') as f:
        tool_config_dict = json.load(f)
    tool_config_id = str(tool_config_dict[pipeline_name])

    #See if the current pipeline has any 'ancestor pipelines', if so
    #we will use this list to be sure that the files that were previously
    #selected when those processing pipelines were ran are still the same
    #files that would be chosen if those pipelines were ran again. If this
    #is not the case, then we will want to pause on processing the current
    #subject until the ancestor pipelines are rerun.
    ancestor_pipeline_file = os.path.join(Path(inspect.getfile(update_processing)).absolute().parent.resolve(), 'ancestor_pipelines.json')
    with open(ancestor_pipeline_file, 'r') as f:
        ancestors_dict = json.load(f)
    ancestor_pipelines = ancestors_dict[pipeline_name]

    #Load the associated_files dictionary, which tells you which files
    #are associated with specific requirements (i.e. jsons for nii.gz, sbrefs, etc.)
    associated_files_file = os.path.join(Path(inspect.getfile(update_processing)).absolute().parent.resolve(), 'associated_files.json')
    with open(associated_files_file, 'r') as f:
        associated_files_dict = json.load(f)    
    
    #Load the processing prerequisites for the current pipeline
    requirements_dicts, file_selection_dict = load_requirements_infos(pipeline_name)
    print('{} requirements dictionaries: {}'.format(pipeline_name, requirements_dicts))

    #If any of the requirements are dependent on QC info, return True
    #otherwise return false and allow processing even when QC file is missing
    qc_info_required = is_qc_info_required(file_selection_dict)
    print("QC info required: {}".format(qc_info_required))

    #Load the processing prerequisites for the ancestor pipelines
    #this will be used to check if the files that were previously
    #selected for processing are still the same files that would
    #be selected if the ancestor pipelines were ran again
    ancestor_pipelines_file_selection_dict = {}
    for temp_ancestor in ancestor_pipelines:
        ancestor_pipelines_file_selection_dict[temp_ancestor] = load_requirements_infos(temp_ancestor)[1]
    print('The following ancestor pipelines will be checked for file selection consistency: {}'.format(ancestor_pipelines))

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
        
    #Grab CBRAIN Tasks that will later be referenced, and seperate them by results data provider ########
    current_cbrain_tasks = find_current_cbrain_tasks(cbrain_api_token, data_provider_id = None) #this is bids_data_provider_id because we currently only support processing that grabs/saves from one DP
    cbrain_session_tasks = {}
    for temp_ses in session_dps_dict.keys():
        temp_dp_id = session_dps_dict[temp_ses]['id']
        cbrain_session_tasks[temp_ses] = list(filter(lambda f: temp_dp_id == f['results_data_provider_id'], current_cbrain_tasks))

    #Grab CBRAIN Files that will later be referenced ##################################################
    cbrain_files = find_cbrain_entities(cbrain_api_token, 'userfiles')
    bids_data_provider_files = list(filter(lambda f: bids_data_provider_id == f['data_provider_id'], cbrain_files))
    cbrain_deriv_files = {}
    print('The following derivative data providers will be used to see if processing is needed + to house the outputs of jobs launched later in the script:')
    for temp_ses in session_dps_dict.keys():
        temp_dp_id = session_dps_dict[temp_ses]['id']
        cbrain_deriv_files[temp_ses] = list(filter(lambda f: temp_dp_id == f['data_provider_id'], cbrain_files))
        print('   Name: {}, ID: {}, Bucket: {}, CBRAIN Defined Prefix: {}'.format(temp_ses, temp_dp_id, session_dps_dict[temp_ses]['bucket'], session_dps_dict[temp_ses]['prefix']))
        print("      {} total files found under data provider".format(len(cbrain_deriv_files[temp_ses])))

    #Print out info about what files were found
    print('\n')
    print('Processing will occur using BidsSubjects under the following DataProvider:\nName: {}, ID: {}, Bucket: {}, User Defined Prefix: {}'.format(bids_data_provider_name, bids_data_provider_id, bids_bucket, bids_bucket_prefix))
    print("      {} total files found under data provider".format(len(bids_data_provider_files)))
    ########################################################################################
    
    registered_and_s3_names, registered_and_s3_ids = find_potential_subjects_for_processing_v2(bids_data_provider_files, bids_bucket_config,
                                                       bids_bucket = bids_bucket, bids_prefix = bids_bucket_prefix)
    print('      Found {} BidsSubjects under DP'.format(len(registered_and_s3_names)))
    
    
    #A list to store details about why some subjects were processed
    #and others were not
    study_processing_details = []
    
    subject_external_requirements_list = []
    all_to_keep_lists = [] #list of lists of files to keep for each subject
    final_subjects_ids_for_proc = [] #list of subjects that fullfill requirements for processing
    final_subjects_names_for_proc = [] #list of subjects that fullfill requirements for processing
    metadata_dicts_list = [] #list for keeping track of s3 file identifiers
    subject_sessions_launched = 0
    for i, temp_subject in enumerate(registered_and_s3_names):
        for j, temp_ses in enumerate(session_dps_dict.keys()):

            #This should always be something like 'ses-V01', 'ses-V02', etc.
            temp_ses_name = session_dps_dict[temp_ses]['prefix'].split('/')[-1]

            #Before gathering information about the current subject and session,
            #check if we have already processed the maximum number of subjects.
            if type(max_subject_sessions_to_proc) != type(None):
                if subject_sessions_launched >= max_subject_sessions_to_proc:
                    break
        
            print('Evaluating: {}'.format(temp_subject))
            
            #A dictionary to store details that will later be used to populate
            #a spreadsheet that will be used to track processing across subjects
            if 'subject_processing_details' in locals():
                study_processing_details.append(subject_processing_details)

            subject_processing_details = {}
            subject_processing_details['subject'] = temp_subject
            subject_processing_details['pipeline'] = pipeline_name
            subject_processing_details['session'] = temp_ses_name
            subject_processing_details['derivatives_found'] = "Not Evaluated"
            subject_processing_details['CBRAIN_Status'] = "Not Evaluated"
            subject_processing_details['scans_tsv_present'] = "Not Evaluated"
            subject_processing_details['Ancestor_Files'] = "Not Evaluated"
            
            #Be sure that the current subject doesn't have existing output before starting processing
            subject_derivatives_prefix = os.path.join(session_dps_dict[temp_ses]['prefix'], pipeline_name, temp_subject) #derivatives_bucket_prefix currently includes session info
            if file_exists_under_prefix(session_dps_dict[temp_ses]['bucket'], subject_derivatives_prefix, derivatives_bucket_config):
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
            to_rerun, example_status = check_rerun_status(registered_and_s3_ids[i], cbrain_session_tasks[temp_ses], session_dps_dict[temp_ses]['id'], tool_config_id, rerun_level = rerun_level)
            subject_processing_details['CBRAIN_Status'] = example_status
            if False == to_rerun:
                continue #Check rerun status will print out a message to the user if processing is not going to be rerun

            #Grab the QC file for this subject so we can figure out which files can be used for processing.
            #If no QC requirements are specified in the comprehensive processing prerequisites, then the QC file will be ignored.
            if type(logs_directory) != type(None):
                subj_ses_qc_file_path = download_scans_tsv_file(bids_bucket_config, logs_directory, temp_subject, temp_ses_name, bids_prefix = bids_bucket_prefix, bucket = bids_bucket, client = None)
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
                    try:
                        os.remove(subj_ses_qc_file_path)
                    except:
                        pass
            else:
                subj_ses_qc_file = None


            #Grab a list of BIDS associated files for this subject in S3
            try:
                subject_files = grab_subject_file_info(temp_subject, bids_bucket_config, bucket = bids_bucket, prefix = bids_bucket_prefix)
            except Exception as error:
                print(error)
                raise RuntimeError('Error: unable to grab file names for S3 for current subject. This likely means S3 access is having issues.')
            if len(subject_files) == 0:
                print('   Warning: No S3 files found for subject')

            #Reduce the files to those that are relevant for
            #the current session being processed
            session_level = len(bids_bucket_prefix.split('/')) + 1 
            session_files = grab_session_specific_file_info(subject_files, temp_ses_name,
                                session_agnostic_files = session_agnostic_files,
                                session_level = session_level)
            if len(session_files) == 0:
                print('   No files found for subject/session combo')
                continue


            #First run preliminary check for requirements that is only used
            #for the purpose of populating the processing details spreadsheet.
            #Later requirements check will actually be used to determine if 
            #processing will be attempted.
            temp_req_output, req_tracking_dict = check_bids_requirements_v2(temp_subject, session_files, file_selection_dict,
                                qc_df = subj_ses_qc_file, bucket = bids_bucket, prefix = bids_bucket_prefix,
                                bids_bucket_config = bids_bucket_config, session = temp_ses_name,
                                session_agnostic_files = session_agnostic_files,
                                verbose = verbose)
            subject_processing_details.update(req_tracking_dict)
                
            #Check that the subject has requirements satisfiying at least one pipeline specific json in the processing_prerequisites folder
            requirements_satisfied = 0
            none_found = 0
            for temp_requirement in requirements_dicts:
                temp_req_output, _ = check_bids_requirements_v2(temp_subject, session_files, temp_requirement,
                            qc_df = subj_ses_qc_file, bucket = bids_bucket, prefix = bids_bucket_prefix,
                            bids_bucket_config = bids_bucket_config, session = temp_ses_name,
                            session_agnostic_files = session_agnostic_files,
                            verbose = verbose)
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
            subject_external_requirements, req_tracking_dict = grab_external_requirements(temp_subject, bids_data_provider_files + cbrain_deriv_files[temp_ses],
                                                                        external_requirements_dict, bids_data_provider_id = bids_data_provider_id,
                                                                        derivatives_data_provider_id = session_dps_dict[temp_ses]['id']) #implement function for this...
            
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
            subject_files_list, metadata_dict = grab_required_bids_files_v2(temp_subject, session_files, file_selection_dict,
                                                                            qc_df = subj_ses_qc_file, bucket = bids_bucket,
                                                                            prefix = bids_bucket_prefix,
                                                                            bids_bucket_config = bids_bucket_config,
                                                                            session = temp_ses_name, session_agnostic_files = session_agnostic_files,
                                                                            associated_files_dict = associated_files_dict,
                                                                            verbose = False)
            
            #Check if all files are old enough for processing. Generally we will
            #want to wait several days before processing to be sure that there is
            #time for the QC information to get populated
            files_old_enough = check_all_files_old_enough(metadata_dict, minimum_file_age_days, 
                                file_patterns_to_ignore = session_agnostic_files,
                                verbose = False)
            if files_old_enough == False:
                print('    Files not old enough for processing')
                subject_processing_details['derivatives_found'] = "No (Files Not Old Enough)"
                subject_processing_details['CBRAIN_Status'] = "No Proc. (Files Not Old Enough)"
                continue
            
            #Go through all "ancestor" processing requirements, and ensure
            #that the files that would be selected for processing today are
            #the same as the files that were selected when the ancestor
            #pipelines were ran. If this is not the case, then processing
            #of this subject will be paused until the ancestor pipelines are rerun.
            if check_ancestor_pipelines:
                if len(ancestor_pipelines) > 0:
                    are_ancestors_the_same = check_if_ancestor_file_selection_is_same(temp_subject, session_files, ancestor_pipelines_file_selection_dict, qc_df = subj_ses_qc_file,
                                                                bids_bucket = bids_bucket, bids_prefix = bids_bucket_prefix, bids_bucket_config = bids_bucket_config,
                                                                session = temp_ses_name, session_agnostic_files = session_agnostic_files, associated_files_dict = associated_files_dict,
                                                                verbose = verbose, derivatives_bucket_config = derivatives_bucket_config, derivatives_bucket = session_dps_dict[temp_ses]['bucket'],
                                                                derivatives_bucket_prefix = session_dps_dict[temp_ses]['prefix'], logs_directory = logs_directory)
                    if are_ancestors_the_same == False:
                        print('    Pausing processing until ancestor pipelines are rerun')
                        subject_processing_details['derivatives_found'] = "No (Ancestor Files Different)"
                        subject_processing_details['CBRAIN_Status'] = "No Proc. (Ancestor Files Different)"
                        subject_processing_details['Ancestor_Files'] = 'Different'
                        continue
                    else:
                        subject_processing_details['Ancestor_Files'] = 'Same'

                
            

            if type(subject_files_list) == type(None): #Guessing this never happens?
                print('    scans.tsv missing one or more acquisitions. Skipping processing.')
                continue

            #Save subject information with other subjects that are ready for processing
            all_to_keep_lists.append(subject_files_list)
            metadata_dicts_list.append(metadata_dict)
            subject_external_requirements_list.append(subject_external_requirements)
            final_subjects_ids_for_proc.append(registered_and_s3_ids[i])
            final_subjects_names_for_proc.append(temp_subject)
            subject_processing_details['CBRAIN_Status'] = 'Initiating Processing'

            #Update the study processing details dict for the last subject
            if 'subject_processing_details' in locals():
                study_processing_details.append(subject_processing_details)

            ########################################################################
            ########################################################################
            #If script gets to this point for a given subject, session, and pipeline,
            #then try to launch a job for processing in CBRAIN.
            #########################################################################
            #########################################################################
            print('    Processing {} with data from {} using {} via API'.format(final_subjects_names_for_proc[-1], temp_ses_name, pipeline_name))


            #Run "mark as newer" to be sure the latest version of the subject data
            #is in the local CBRAIN cache once processing begins##################
            try:
                for temp_key in subject_external_requirements_list[-1].keys():
                    cbrain_mark_as_newer(subject_external_requirements_list[-1][temp_key], cbrain_api_token)                

                #Launch Processing
                status, json_for_logging = launch_task_concise_dict(pipeline_name, subject_external_requirements_list[-1], cbrain_api_token, data_provider_id = session_dps_dict[temp_ses]['id'],
                                            group_id = group_id, user_id = user_id, task_description = '{} via API'.format(final_subjects_names_for_proc[-1]),
                                            all_to_keep = all_to_keep_lists[-1], session_label = temp_ses_name.split('-')[1])
            except:
                print('Error encountered while trying to submit job for processing. This is likely a networking issue. Will try again in 5 seconds.')
                time.sleep(5) #wait 5 seconds and try again
                for temp_key in subject_external_requirements_list[-1].keys():
                    cbrain_mark_as_newer(subject_external_requirements_list[-1][temp_key], cbrain_api_token)                

                #Launch Processing
                status, json_for_logging = launch_task_concise_dict(pipeline_name, subject_external_requirements_list[i], cbrain_api_token, data_provider_id = session_dps_dict[temp_ses]['id'],
                                            group_id = group_id, user_id = user_id, task_description = '{} via API'.format(final_subjects_names_for_proc[-1]),
                                            all_to_keep = all_to_keep_lists[-1], session_label = temp_ses_name.split('-')[1])
            #######################################################################
            
            json_for_logging['s3_metadata'] = metadata_dicts_list[-1]
            if status == False:
                raise ValueError('Error CBRAIN processing tasked was not submitted for {}. Issue must be resolved for processing to continue.'.format(final_subjects_names_for_proc[-1]))
            else:
                if type(logs_directory) != type(None):
                    log_file_name = os.path.join(logs_directory, '{}_{}_UMNProcSubmission.json'.format(final_subjects_names_for_proc[-1], pipeline_name))
                    with open(log_file_name, 'w') as f:
                        json.dump(json_for_logging, f, indent = 4)
                    #derivatives_bucket_prefix
                    upload_processing_config_log(log_file_name, bucket = session_dps_dict[temp_ses]['bucket'], prefix = os.path.join(session_dps_dict[temp_ses]['prefix'], logs_prefix), bucket_config = derivatives_bucket_config)
                    os.remove(log_file_name)

            if type(max_subject_sessions_to_proc) != type(None):
                subject_sessions_launched += 1
            
       
    #################################################################################################
    #################################################################################################
    #Iterate through all subjects who were deemed ready for processing,
    # and submit task to process their data in CBRAIN.
    
    study_tracking_df = pd.DataFrame.from_dict(study_processing_details)
    if type(logs_directory) != type(None):
        log_csv_name = os.path.join(logs_directory, 'processing_details_{}.csv'.format(pipeline_name))
        log_html_name = os.path.join(logs_directory, 'processing_details_{}.html'.format(pipeline_name))
        study_tracking_df = html_tools.reformat_df_and_produce_proc_html(study_tracking_df, pipeline_name, log_html_name, file_selection_dict)
        study_tracking_df.to_csv(log_csv_name, index = False)

    
    return study_tracking_df