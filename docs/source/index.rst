.. HBCD_CBRAIN_PROCESSING documentation master file, created by
   sphinx-quickstart on Wed Jun  5 10:48:12 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to the Healthy Brain and Child Development (HBCD) Study's Processing Documentation!
===========================================================================================

.. warning::
    Warning - this documentation is a work in progress
    and details are subject to change at any moment.

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


How to interact with this documentation
---------------------------------------

There are a number of pages within this document that are intended to better oriented
users to how HBCD processing is conducted. Here we give an overview of some of the pages
that may contain information you are interested in.

- :doc:`BIDS Details <bids_details>`: This page contains high-level details about the BIDS curation process as it
  relates to HBCD processing.
- :doc:`Tool Details <tool_details>`: The links within this page contain automatically rendered information about
  the different image processing pipelines that are used for HBCD processing. These links also contain information
  about how we select which files a given pipeline should be exposed to, which arguments are provided to the pipeline,
  and what the expected outputs are.
- :doc:`Boutiques Descriptors <boutiques_descriptors>`: This page contains information about Boutiques descriptors,
  which are the JSON files that tells CBRAIN how a pipeline should be interacted with. A given version of a tool generally
  has one descriptor, and this descriptor will be used to launch many processing jobs on different subjects. Understanding
  some of the high-level details behind boutiques descriptors is important for understanding many of the fields in the Tool
  Details section.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   index
   bids_details
   tool_details
   boutiques_descriptors
   references
