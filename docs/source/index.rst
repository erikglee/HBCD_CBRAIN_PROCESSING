.. HBCD_CBRAIN_PROCESSING documentation master file, created by
   sphinx-quickstart on Wed Jun  5 10:48:12 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to the processing documentation for the Healthy Brain and Child Development (HBCD) Study!
=================================================================================================

This documentation base is an overview of the tools and
processing steps used in the HBCD study. At a high level,
HBCD processing involves the following details:

1. **BIDS Curation**: MRI, EEG, and Biosensors data from HBCD
   is curated in BIDS format by LORIS, and made available in
   an S3 bucket.
2. **Pipeline Organization**: Pipelines that are used for HBCD
   processing are ingested into CBRAIN. Each pipeline will have
   a container with all the prerequisites for processing, and also
   a Boutiques Descriptor that describes how a container should be
   interacted with during processing.
3. **Identifying Subjects/Sessions/Files For Processing**: For each
   pipeline there are specific file requirements that must be satisfied
   for a subject to be processed. In some cases this means a certain modality
   must be present, in other cases it also means that data must satisfy QC criteria.
4. **Running Pipelines**: In HBCD, processing always occurs on one session's worth
   of data for a given subject. Once step 3 is satisfied for a session, CBRAIN will
   grab the files for a given session that are necessary for processing and initiate
   processing.
5. **Data Management**: For processing that has been finished successfully, CBRAIN
   will send the pipeline outputs back to S3. For processing that is unsuccesful,
   CBRAIN will maintain a record of the error and the files that were used for processing.



.. toctree::
   :maxdepth: 1
   :caption: Contents:

   bids_details
   tool_details
