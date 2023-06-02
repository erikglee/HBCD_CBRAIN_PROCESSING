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
    page_iterator = create_page_iterator(bucket = bucket, prefix = prefix, bids_bucket_config = bids_bucket_config)

    #Iterate through bucket to find potential subjects
    s3_contents = []
    potential_subjects = []
    #potential_subjects_dates = []
    for page in page_iterator:
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


def create_page_iterator(bucket = 'hbcd-pilot', prefix = 'derivatives', bids_bucket_config = False):
    '''Utility to create a page iterator for s3 bucket'''
    
    #Grab config path
    if bids_bucket_config == False:
        config_path = ''
    else:
        if type(bids_bucket_config) != str:
            raise NameError('Error: different config path should eithe be string or boolean')
        else:
            config_path = bids_bucket_config
            
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
        
    # Create a reusable Paginator
    paginator = client.get_paginator('list_objects')
        
    # Create a PageIterator from the Paginator
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    return page_iterator

def upload_cbrain_csv_file(file_name, bucket = 'hbcd-cbrain-test', prefix = 'cbrain_misc/cbrain_csvs', bucket_config = False):
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
    print(object_name)

    # Upload the file
    try:
        response = client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def check_bids_requirements(subject_id, requirements_dict, bucket = 'hbcd-pilot', prefix = 'assembly_bids', bids_bucket_config = False):
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
    
    # Do some cosmetics so that the prefix includes the pipeline
    # name and so that later we look for the subject ID at the
    # right level
    full_prefix = os.path.join(prefix, subject_id)
    prefix_offset = len(full_prefix.split('/')) - 1
    if len(prefix) == 0:
        prefix_offset = prefix_offset - 1
        
    # Create a PageIterator from the Paginator
    page_iterator = create_page_iterator(bucket = bucket, prefix = full_prefix, bids_bucket_config = bids_bucket_config)

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
        child_requirements = requirements_dict[parent_requirement]
        for temp_file in subject_files:
            child_requirements_satisfied = 0
            for child_requirement in child_requirements.keys():
                if (child_requirement in temp_file) == child_requirements[child_requirement]:
                    child_requirements_satisfied += 1
                else:
                    break
            if child_requirements_satisfied == len(child_requirements.keys()):
                parent_requirements_satisfied += 1
                break
    
    #If all requirements have been satisfied at least once return true, else false
    if parent_requirements_satisfied == len(requirements_dict.keys()):
        return True
    else:
        return False
    
    
def grab_required_bids_files(subject_id, requirements_dict, num_requirements_dict, bucket = 'hbcd-pilot', prefix = 'assembly_bids', bids_bucket_config = False):
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
    page_iterator = create_page_iterator(bucket = bucket, prefix = full_prefix, bids_bucket_config = bids_bucket_config)

    #Iterate through bucket to find BIDS files for this subject
    subject_files = []
    for page in page_iterator:
        if 'Contents' in page:
            for temp_dict in page['Contents']:
                subject_files.append('/'.join(temp_dict['Key'].split('/')[-2:]))
                #print(temp_dict['Key'])
        else:
            print('No BIDS contents found for: {}'.format(subject_id))

    subject_files.sort()
    output_file_list = []
    parent_requirements_satisfied = 0
    for i, parent_requirement in enumerate(requirements_dict.keys()):
        temp_requirement_file_list = []
        child_requirements = requirements_dict[parent_requirement]
        for temp_file in subject_files:
            child_requirements_satisfied = 0
            for child_requirement in child_requirements.keys():
                if (child_requirement in temp_file) == child_requirements[child_requirement]:
                    child_requirements_satisfied += 1
                else:
                    break
            if child_requirements_satisfied == len(child_requirements.keys()):
                temp_requirement_file_list.append(temp_file)
        
        counter = 0
        for temp_file in temp_requirement_file_list:
            if num_requirements_dict[parent_requirement] == 'all':
                output_file_list.append(temp_file)
            elif num_requirements_dict[parent_requirement] > counter:
                output_file_list.append(temp_file)
                counter += 1
            else:
                print('Finishing Requirement {}, {} files already found.'.format(parent_requirement, counter))
                
    return output_file_list

def make_cbrain_csv_text(subject_cbrain_id, subject_bids_id, size, data_provider_name, cbrain_user_name, cbrain_group_name, files_list):
    """Internal function to create text for a cbrain csv file
    
    Creates text for a CBRAIN CSV file or extended file list.
    This text will still need to be saved to a file and uploaded/
    registered in CBRAIN.
    
    Parameters
    ----------
    subject_cbrain_id : str
        The CBRAIN ID of the subject
    subject_bids_id : str
        The BIDS ID of the subject
        (i.e. sub-01)
    size : str
        The size of the subject's data
        (this is grabbed from CBRAIN)
    data_provider_name : str
        The name of the CBRAIN data provider.
        This is different than the numeric ID!
    cbrain_user_name : str
        The name of the CBRAIN user that is
        doing processing.
    cbrain_group_name : str
        The name of the CBRAIN group that this subject
        belongs to
    files_list : list
        List of files to be associated with the subject
        in this CBRAIN CSV file.
    
    Returns
    -------
    str
        Text for a CBRAIN CSV file or extended file list.
    
    """
    
    text_template_pt1 = '{ID},"{sub}",{size},"BidsSubject","{data_provider}","{user_name}","{group_name}",'
    text_template_pt2 = 'all_to_keep"":{file_insert_text}'
    
    file_insert_text = '[""'
    for temp_file in files_list:
        file_insert_text = file_insert_text + temp_file + '"",""'
    file_insert_text = file_insert_text[:-3] + ']'
    final_text = text_template_pt1.format(ID=subject_cbrain_id,sub=subject_bids_id,data_provider=data_provider_name, user_name=cbrain_user_name, group_name=cbrain_group_name, size=size)
    final_text = final_text + '"{""' + text_template_pt2.format(file_insert_text=file_insert_text) + '}"\n'
    return final_text


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
        #headers = {'Content-Type': 'application/json'},
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
    
    
def check_if_derivatives_exist(subject_name, pipeline_folder, bucket = 'hbcd-pilot', prefix = 'derivatives', bids_bucket_config = False):
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
    page_iterator = create_page_iterator(bucket = bucket, prefix = full_prefix, bids_bucket_config = bids_bucket_config)
    for page in page_iterator:
        if 'Contents' in page:
            if len(page['Contents']):
                return True
        else:
            return False
    
def grab_json(json_config_location, pipeline_name):
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
    return json_contents


def construct_generic_cbrain_task_info_dict(cbrain_api_token, group_id, user_id, tool_config_id, data_provider_id, task_description, variable_parameters_dict, fixed_parameters_dict):
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

    #Here for testing
    print(task_data['cbrain_task']['params']['interface_userfile_ids'])
    
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
        print("Successfully submitted {} processing to CBRAIN for CBRAIN CSV File.".format(pipeline_name))
        task_info = task_response.json()
        print(json.dumps(task_info, indent=4)) #Could return this if we want to use it later...
        return True
    else:
        print("Failed to submit {} processing to CBRAIN for CBRAIN CSV File with ID.".format(pipeline_name))
        print(task_response.text)
        return False
    


def launch_task_concise_dict(pipeline_name, variable_parameters_dict, cbrain_api_token, data_provider_id = 710,
                             override_tool_config_id = False, group_id = 10367, user_id = 4022, task_description = '',
                             custom_json_config_location = False):

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
        The numeric id of the data provider on CBRAIN. In this case, both the input/output
        data will be using the same data_provider_id
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
    fixed_parameters_dict = grab_json(custom_json_config_location, pipeline_name)
        
    #Construct different dictionaries that will be sent to CBRAIN
    task_headers, task_params, task_data = construct_generic_cbrain_task_info_dict(cbrain_api_token, group_id, user_id, tool_config_id, data_provider_id, task_description, variable_parameters_dict, fixed_parameters_dict)
        
    #Submit task to CBRAIN
    return submit_generic_cbrain_task(task_headers, task_params, task_data, pipeline_name)


#Not being used but is still probably useful
def find_current_cbrain_tasks(cbrain_api_token):
    '''Generates info on extended file list files
    
    Parameters
    ----------
    
    cbrain_api_token : str
        The CBRAIN API token for the current session
        
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
    
    return tasks


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


def grab_external_requirements(subject_name, cbrain_files, requirements_dict):
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
        Files on the data provider of interest
    requirements_dict : dict
    
    Returns
    -------
        None if some requirements from requirements_dict were not
        found, otherwise returns a dictionary with the ids for the
        different files
    
    
    '''
    
    subject_external_requirements = {}
    for temp_requirement in requirements_dict.keys():
        requirement_found = False
        for temp_file in cbrain_files:
            if (temp_file['name'] == subject_name) and (temp_file['type'] == requirements_dict[temp_requirement]):
                requirement_found = True
                subject_external_requirements[temp_requirement] = temp_file['id']
                break
        if requirement_found == False:
            return None
    return subject_external_requirements

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
    print('Total identified subjects: {},\nSubjects Still Needing to Be Registered in CBRAIN: {},\nSubjects Registered in CBRAIN and in S3 (ready to process): {},\nRegistered Subjects Not in S3 (this should be 0): {}'.format(len(total_subjects), len(non_registered_subjects), len(registered_and_s3), len(missing_in_s3_subjects)))

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
    True if processing is recommended, else False
    
    
    '''

    rerun_group_1 = ['Terminated', 'Failed To Setup', 'Failed To PostProcess', 'Failed Setup Prerequisites', 'Failed PostProcess Prerequisites']
    rerun_group_2 = ['Suspended', 'Failed', 'Failed On Cluster']

    task_statuses = []
    task_ids = []
    for temp_task in cbrain_tasks:
        if temp_task['tool_config_id'] == tool_config_id:
            if temp_task['results_data_provider_id'] == derivatives_data_provider_id:
                try:
                    if cbrain_subject_id in temp_task['params']['interface_userfile_ids']:
                        print('hello')
                        task_statuses.append(temp_task['status'])
                        task_ids.append(temp_task['id'])
                except:
                    continue

    num_rerun_group_1 = 0
    num_rerun_group_2 = 0
    for temp_status in task_statuses:
        if temp_status in rerun_group_1:
            num_rerun_group_1 += 1
        if temp_status in rerun_group_2:
            num_rerun_group_2 += 1

    if len(task_statuses) == 0:
        return True
    else:
        if rerun_level == 0:
            print('    Found existing task(s), consider deleting the task(s) or using higher rerun level. Tasks: {}, Statuses: {}'.format(task_ids, task_statuses))
        elif rerun_level == 1:
            if len(task_statuses) == num_rerun_group_1:
                return True
            else:
                print('    Found existing task(s) with status within rerun_level = 1, consider deleting the task(s) or using higher rerun level. Tasks: {}, Statuses {}'.format(task_ids, task_statuses))
        elif rerun_level == 2:
            if len(task_statuses) == (num_rerun_group_1 + num_rerun_group_2):
                return True
            else:
                print('    Found existing task(s) with status within rerun_level = 2, consider deleting the task(s) or using higher rerun level. Tasks: {}, Statuses {}'.format(task_ids, task_statuses))
        else:
            raise ValueError('Error: rerun_level must be 0, 1, or 2')

    return False


def update_processing(pipeline_name, registered_and_s3_names, registered_and_s3_ids, registered_and_s3_sizes, cbrain_csv_file_dir,
                      cbrain_api_token, bids_data_provider_name = 'HBCD-Pilot-Official', user_name = 'elee', group_name = 'HBCD-Computing',
                      group_id = '10367', user_id = '4022', raise_error_for_duplicate_cbrain_csv_files = False,
                      bids_bucket_config = '/some/path', bids_bucket = 'hbcd-pilot', bids_bucket_prefix = 'assembly_bids',
                      derivatives_bucket_config = '/some/path', derivatives_bucket = 'hbcd-pilot', derivatives_bucket_prefix = 'derivatives',
                      bids_data_provider_id = '710', cbrain_logging_folder_prefix='cbrain_misc', rerun_level = 1):
    
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
    registered_and_s3_sizes: list of str
        Has the same ordering of registered_and_s3_names, and contains
        the sizes of each subject's data (taken from CBRAIN)
    cbrain_csv_file_dir : str
        The path to the directory where cbrain file lists will be saved locally
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
    raise_error_for_duplicate_cbrain_csv_files: bool, default True
        If this is true, the script will throw an error if one of the cbrain file
        list files that are going to be registered already exists in CBRAIN. If this
        is false, then CBRAIN will not throw an error but will only process subjects
        who don't already have files registered in CBRAIN.
    rerun_level: 0, 1, 2, default 1
        If 0, then the script will not rerun any subjects that already have
        processing results in CBRAIN. If 1, then the script will rerun subjects
        that have processing results in CBRAIN but only if the processing results
        failed for CBRAIN/Cluster specific reasons. If 2, then the script will 
        also rerun processing in cases where processing failed on the cluster or
        in similar scenarios.
    
    '''

    #Load the tool config id for the current pipeline being used for processing
    tool_config_file = os.path.join(Path(inspect.getfile(update_processing)).absolute().parent.resolve(), 'tool_config_ids.json')
    with open(tool_config_file, 'r') as f:
        tool_config_dict = json.load(f)
    tool_config_id = str(tool_config_dict[pipeline_name])
    
    
    #Load different json files that are the same for each subject
    requirements_files = glob.glob(os.path.join(Path(inspect.getfile(update_processing)).absolute().parent.resolve(), 'processing_prerequisites','{}*.json'.format(pipeline_name)))
    requirements_dicts = []
    for temp_requirement_file in requirements_files:
        with open(temp_requirement_file, 'r') as f:
            requirements_dicts.append(json.load(f))
    print('{} requirements dictionaries: {}'.format(pipeline_name, requirements_dicts))
            
    file_selection_file_path = os.path.join(Path(inspect.getfile(update_processing)).absolute().parent.resolve(), 'processing_file_selection', '{}.json'.format(pipeline_name))
    file_numbers_file_path = os.path.join(Path(inspect.getfile(update_processing)).absolute().parent.resolve(), 'processing_file_numbers', '{}.json'.format(pipeline_name))
    external_requirements_file_path = os.path.join(Path(inspect.getfile(update_processing)).absolute().parent.resolve(), 'external_requirements', '{}.json'.format(pipeline_name))

    with open(file_selection_file_path, 'r') as f:
        file_selection_dict = json.load(f)
    with open(file_numbers_file_path, 'r') as f:
        file_numbers_dict = json.load(f)
    with open(external_requirements_file_path, 'r') as f:
        external_requirements_dict = json.load(f)
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
    current_cbrain_tasks = find_current_cbrain_tasks(cbrain_api_token)
    data_provider_files = find_cbrain_files_on_dp(cbrain_api_token, data_provider_id = bids_data_provider_id)
    ########################################################################################
    
    
    cbrain_csv_names = [] #cbrain name
    cbrain_csv_local_paths = [] #local path
    subject_external_requirements_list = []
    for i, temp_subject in enumerate(registered_and_s3_names):
        
        print('Evaluating: {}'.format(temp_subject))
        
        #Be sure that the current subject doesn't have existing output before starting processing
        if check_if_derivatives_exist(temp_subject, pipeline_name,
                                        bucket = derivatives_bucket, prefix = derivatives_bucket_prefix,
                                        bids_bucket_config = derivatives_bucket_config):
            print('    Already has derivatives')
            continue

        #Check what type of processing has already occured for the subject with
        #this pipeline and only continue if processing hasn't already been initiated
        #or under certain failure conditions (see documentation for check_rerun_status)
        if False == check_rerun_status(registered_and_s3_ids[i], current_cbrain_tasks, bids_data_provider_id, tool_config_id, rerun_level = 1):
            print('    Existing CBRAIN tasks indicate that processing is unnecessary, but this isnt used yet...')
            
            
        #Check that the subject has requirements satisfiying at least one pipeline specific json in the processing_prerequisites folder
        requirements_satisfied = 0
        for temp_requirement in requirements_dicts:
            requirements_satisfied += int(check_bids_requirements(temp_subject, temp_requirement, bucket = bids_bucket, prefix = bids_bucket_prefix, bids_bucket_config = bids_bucket_config))
        if requirements_satisfied == 0:
            print('    Requirements not satisfied')
            continue
            
        #Check that the external requirements are satisfied for the subject (these are pipeline inputs that will be files/file collections
        #that should already be available for the subject on CBRAIN if the subject is ready for processing)
        subject_external_requirements = grab_external_requirements(temp_subject, data_provider_files, external_requirements_dict) #implement function for this...
        if subject_external_requirements is None:
            print('    Missing external requirements')
            continue #skip processing if external requirements aren't found
            
        #Grab files for the subject according to pipeline specific jsons in processing_file_numbers and processing_file_selection folders
        subject_files_list = grab_required_bids_files(temp_subject, file_selection_dict, file_numbers_dict, bucket = bids_bucket, prefix = bids_bucket_prefix, bids_bucket_config = bids_bucket_config)
        print('    Will attempt to run processing if this subject doesnt already have CBRAIN CSV for current pipeline registered in CBRAIN')
        
        text = make_cbrain_csv_text(registered_and_s3_ids[i], temp_subject, registered_and_s3_sizes[i], bids_data_provider_name, user_name, group_name, subject_files_list)
        cbrain_csv_file_path = os.path.join(cbrain_csv_file_dir, '{}_{}_CbrainId-{}.csv'.format(pipeline_name, temp_subject, registered_and_s3_ids[i]))
        with open(cbrain_csv_file_path, 'w') as f:
            f.write(text)
        cbrain_csv_local_paths.append(cbrain_csv_file_path)
        cbrain_csv_names.append('{}_{}_CbrainId-{}.csv'.format(pipeline_name, temp_subject, registered_and_s3_ids[i]))
        subject_external_requirements_list.append(subject_external_requirements)
       
    #################################################################################################
    #################################################################################################
    #Check that the CBRAIN CSV files don't already exist in CBRAIN before
    #registering them.. then register the ones that didn't exist. Either
    #throw an error or register new files depending on the value of raise_error_for_duplicate_cbrain_csv_files
    already_registered_cbrain_csv_files = []
    unique_cbrain_csv_names = []
    unique_cbrain_csv_local_paths = []
    unique_subject_external_requirements = []
    for i, temp_cbrain_csv in enumerate(cbrain_csv_names):
        duplicate_found = False
        for temp_file in data_provider_files:
            if temp_cbrain_csv == temp_file['name']:
                already_registered_cbrain_csv_files.append(temp_cbrain_csv)
                duplicate_found = True
        if duplicate_found == False:
            unique_cbrain_csv_names.append(temp_cbrain_csv)
            unique_cbrain_csv_local_paths.append(cbrain_csv_local_paths[i])
            unique_subject_external_requirements.append(subject_external_requirements_list[i])
    print('CBRAIN CSV NAMES')
    print(cbrain_csv_names)
    if len(already_registered_cbrain_csv_files) and (raise_error_for_duplicate_cbrain_csv_files == True):
        raise ValueError('Error: A number of CBRAIN CSV files with the same names are already registered in CBRAIN. Attempting processing will use existing CBRAIN files. Go to CBRAIN and delete files for: {}'.format(cbrain_csv_names))
    elif len(already_registered_cbrain_csv_files) and (raise_error_for_duplicate_cbrain_csv_files == False):
        print('Skipping processing for the following files since they were already registered in CBRAIN before initiation of this script. If you want these files to be included in processing, go to the CBRAIN GUI and delete them. Then if you run this script again the previously deleted files will be recreated for processing: {}'.format(already_registered_cbrain_csv_files))
    print('CBRAIN csv files created that werent already on CBRAIN {}'.format(unique_cbrain_csv_names))
    for i, temp_cbrain_csv in enumerate(unique_cbrain_csv_names):
            
        #Upload the CBRAIN CSV File to S3 and then Register it in CBRAIN
        upload_cbrain_csv_file(unique_cbrain_csv_local_paths[i], 
                               bucket = derivatives_bucket, 
                               prefix = os.path.join(cbrain_logging_folder_prefix, 'cbrain_csvs'), 
                               bucket_config = bids_bucket_config)
        register_cbrain_csvs_in_cbrain(temp_cbrain_csv, 
                                cbrain_api_token, 
                                user_id = user_id, 
                                group_id = group_id, 
                                browse_path = os.path.join(cbrain_logging_folder_prefix, 'cbrain_csvs'), 
                                data_provider_id = bids_data_provider_id)
    #####################################################################################################
    #####################################################################################################
               
    #Find all extended file lists in our DP in CBRAIN and start processing with the ones we just uploaded
    cbrain_extended_file_list_results = find_cbrain_extended_file_list_files(cbrain_api_token, data_provider_id = bids_data_provider_id)
    for i, temp_cbrain_csv in enumerate(unique_cbrain_csv_names):
        for temp_file in cbrain_extended_file_list_results:
            if temp_cbrain_csv.split('/')[-1] == temp_file['name']:
                
                #Replace the BidsSubject field with the CBRAIN CSV ID. Importantly
                #this will only work if the subject has one field that takes the
                #file type BidsSubject.
                for temp_requirement in external_requirements_dict.keys():
                    if external_requirements_dict[temp_requirement] == 'BidsSubject':
                        print('TEMP FILE: ')
                        print(temp_file)
                        unique_subject_external_requirements[i][temp_requirement] = str(temp_file['id'])
                print('{} via API'.format(temp_file['name']))
                launch_task_concise_dict(pipeline_name, unique_subject_external_requirements[i], cbrain_api_token, data_provider_id = bids_data_provider_id,
                                         group_id = group_id, user_id = user_id, task_description = '{} via API'.format(temp_file['name']))

    return data_provider_files