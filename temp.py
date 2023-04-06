cbrain_api_token = '951b9e5b9f16adef0ca24ffec5734fbf' #Go to your account page to generate a new CBRAIN API token
s3_subjects = find_s3_subjects(bids_bucket_config) #Will need to change the bucket pointed to here later

#Grab data provider id for bucket from cbrain web instance
cbrain_subjects, ids, sizes = find_cbrain_subjects(cbrain_api_token, data_provider_id = 710) #710 = HBCD Pilot Official, #725 is old

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


cbrain_csv_file_dir = '/home/umii/leex6144/Documents/Notebooks/CBRAINS_BOUTIQUE/CBRAIN_CSV/cbrain_csv_files_dir/'

update_fully_generic_processing_using_csv_dict('mriqc', registered_and_s3_names, registered_and_s3_ids, registered_and_s3_sizes, cbrain_csv_file_dir,
                      cbrain_api_token, data_provider_name = 'HBCD-Pilot-Official', user_name = 'elee', group_name = 'HBCD-Computing',
                      bucket = 'hbcd-pilot', data_provider_id = 710, group_id = 10367, user_id = 4022, raise_error_for_duplicate_cbrain_csv_files = False)