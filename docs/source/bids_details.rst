.. HBCD_CBRAIN_PROCESSING documentation master file, created by
   sphinx-quickstart on Wed Jun  5 10:48:12 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Relevant BIDS Details for HBCD Processing
=========================================

As much as possible, HBCD processing tries to utilize the `Brain
Imaging Data Structure  <https://bids-specification.readthedocs.io/en/stable/>`_
(BIDS) standard for data organization. Because of this, the applications
used to process HBCD data are also designed to be `BIDS-Apps <https://bids-apps.neuroimaging.io/>`_.

In this section, we describe some of the key elements of HBCD BIDS organization
as it pertains to processing. At a high level, the HBCD BIDS structure will appear
as follows: ::

    assembly_bids/
    ├── participants.tsv
    ├── participants.json
    ├── sub-<label>/
    │   ├── sub-<label>_sessions.tsv
    │   ├── sub-<label>_sessions.json
    │   ├── ses-<label>/
    │   │   ├── anat/
    │   │   ├── dwi/
    │   │   ├── eeg/
    │   │   ├── fmap/
    │   │   ├── func/
    │   │   ├── motion/
    │   │   ├── mrs/
    │   │   ├── sub-<label>_ses-<label>_scans.tsv
    │   │   ├── sub-<label>_ses-<label>_scans.json

As anticipated in a large infant study, many subjects will have
missing data elements. As a result, the number of folders and files
available for each unique subject and session will vary.
Second, because the HBCD acquisition involves multiple modalities,
some are collected at different times. Even within a single modality,
certain acquisitions may be gathered on different days.

The complexity of data acquisition and the varying image quality across scans
make the scans.tsv file, located in the session folder, a vital part of the
BIDS structure. This file contains information about when an acquisiton was
collected, how old the participant was at the time of the acquisition, and in
certain cases there is also information about the quality of the underlying
acquisition. To get a better understanding of what the different fields in the
scans.tsv file mean, please refer to the scans.json file.

The scans.tsv serves as the best source of information about the age of a
participant at the time of an acquisition. Age information can also be found
in the sessions.tsv file under the session folder, where "age" represents the
age of the participant at the first in-person data collection. All "age" measures
are provided in years with three decimal places, based on a birthdate measure
that is jittered up to 7 days.

When processing begins, scripts first examine the S3 structure where the
BIDS data is stored to identify the available files for a given subject.
Each pipeline has specific requirements based on the type of processing
being done (e.g., QSIPREP requires diffusion data, while MADE needs EEG data).
In certain cases, like Magnetic Resonance Spectroscopy (MRS) and
Electroencephalography (EEG), the pipeline run in CBRAIN is used to generate
QC measures. In these cases, all files with unknown QC status are fed to the
pipeline. In other instances, preliminary QC measures
are already in place to assess whether the data is of sufficient quality for
processing or to prioritize which data should be processed first. In these
cases where QC is available, the scans.tsv file is downloaded and queried
to determine which files should be included or prioritized for processing.

Structure of Processing Outputs
-------------------------------

Given the longitudinal nature of the HBCD study, we regularly
have new data coming in for a given subject. To manage ongoing
processing of data that is continually being added to the
dataset, the default processing structure sends outputs to
session-specific folders. For example, the outputs of a pipeline
take the form: ::
   
   #Note! This may not be what you are exposed to as a user...
   #      read further for more details...
   derivatives/ses-<label>/<pipeline_name>/sub-<label>/ses-<label>/...
   
In CBRAIN, these session-specific output folders are
considered unique "Data Providers". You may see this type
of information being utilized when you look at the various
tool descriptions and boutiques descriptors for a given pipeline: ::

   derivatives/ses-<label>

In HBCD processing the "Data Providers" generally refer to different
paths within a single S3 Bucket. For example, the BIDS data, and all
the session-specific derivatives folders are stored within the same
S3 bucket, with each being represented as a different "Data Provider".

This partitioning of derivatives into session-specific folders is
done to ensure the integrity of ongoing processing. For end users
who are instead interacting with a one-time data dump of the HBCD
data, the outputs will be re-organized into a single derivatives
structure that contains the outputs for all sessions worth of
processing. For example, this means that the first two folders seen
below would be merged to create the third folder: ::

   #Note! This is what you will see as a user...
   derivatives/ses-<label>/<pipeline_name>/sub-<label>/...
   derivatives/ses-<label>/<pipeline_name>/sub-<label>/...
   derivatives/<pipeline_name>/sub-<label>/...




.. toctree::
   :maxdepth: 2
   :caption: Contents: