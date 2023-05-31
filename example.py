import cbrain_proc

cbrain_api_token = ''
bids_bucket_config = ''
derivatives_bucket_config = bids_bucket_config
bids_bucket = '' #Could this be inferred from the data provider ID? Would that reqiore DP being at base layer of bucket?
derivatives_bucket = bids_bucket #Could this be inferred from the data provider ID?
bids_prefix = 'assembly_bids'
derivatives_bucket_prefix = 'derivatives'
bids_data_provider_id = ''
bids_data_provider_name = '' #Could this be inferred from the data provider ID? (or vice versa)
pipeline_name = 'mriqc'
cbrain_csv_file_dir = '/home/umii/leex6144/Documents/Notebooks/CBRAINS_BOUTIQUE/CBRAIN_CSV/cbrain_csv_files_dir/'
user_name = 'elee'
user_id = '4022' #Could this be inferred from the user name? When is this needed?
group_name = 'HBCD-Computing'
group_id = '10367' #Could this be inferred from the group name? When is this needed?
cbrain_logging_folder_prefix = 'cbrain_misc'

names, ids, sizes = cbrain_proc.find_potential_subjects_for_processing(cbrain_api_token, 
                                                                        bids_bucket_config, 
                                                                        bids_bucket = bids_bucket,
                                                                        bids_prefix = bids_prefix, 
                                                                        data_provider_id = bids_data_provider_id)

cbrain_proc.update_processing(pipeline_name, names, ids, sizes, cbrain_csv_file_dir,
                      cbrain_api_token, bids_data_provider_name = bids_data_provider_name, user_name = user_name, group_name = group_name,
                      group_id = group_id, user_id = user_id, raise_error_for_duplicate_cbrain_csv_files = False,
                      bids_bucket_config = bids_bucket_config, bids_bucket = bids_bucket, bids_bucket_prefix = bids_prefix,
                      derivatives_bucket_config = derivatives_bucket_config, derivatives_bucket = derivatives_bucket, derivatives_bucket_prefix = derivatives_bucket_prefix,
                      bids_data_provider_id = bids_data_provider_id, cbrain_logging_folder_prefix=cbrain_logging_folder_prefix)
