Tool Details Overview
=====================

The pages linked towards the bottom provide information about the tools used
for HBCD processing. Details including which arguments
are used for processing, and which files are required for processing
are also found in these pages.

These pages are intended to provide a general reference for users who are
interested in understanding how processing is conducted. All pages
that you see below are auto-populated based on the processing configuration
files in the `HBCD_CBRAIN_PROCESSING repository <https://github.com/erikglee/HBCD_CBRAIN_PROCESSING>`_,
and versions of the Boutiques descriptors that we have stored in CBRAIN.
For HBCD datasets that are processed in CBRAIN, similar information is
also automatically saved out to the subject level derivatives folder.
For example, for a given pipeline there is the following structure: ::
    
    pipeline_name/
    ├── sub-<label>/
    │   ├── .cbrain/
    │   │   ├── boutiques_descriptor.json
    │   │   ├── boutiques_invoke.json
    │   │   ├── cbrain_params.json
    │   │   ├── job_script.sh
    │   │   ├── runtime_info.properties
    │   │   ├── stderr.log
    │   │   ├── stdout.log
    │   ├── ... (other processing outputs)

Within the files above, users are provided with the following information:

- The boutiques_descriptor.json file will contain the exact version of the boutiques descriptor that was used for processing.
- The boutiques_invoke.json will describe the arguments used for processing.
- The cbrain_params.json file will specify details including which files were used for processing.
  While this document does not contain the QC criteria that was used to include/exclude files from
  processing, it does contain the files that were chosen as a result of this process.
- The job_script.sh file has certain details about how CBRAIN configured processing on Minnesota Supercomputing Institute's
  cluster.
- The runtime_info.properties file will contain information about the processing script that was run.
- The stderr.log and stdout.log files will contain the standard error and standard output from the processing job.

Tool Names
~~~~~~~~~~