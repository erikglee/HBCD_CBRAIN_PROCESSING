.. HBCD_CBRAIN_PROCESSING documentation master file, created by
   sphinx-quickstart on Wed Jun  5 10:48:12 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Installation
============

The intended use of this pipeline is through the use of a `Singularity <https://docs.sylabs.io/guides/3.7/user-guide/index.html>`_  or `Docker <https://docs.docker.com/get-started/>`_
image. The image can be built using the Dockerfile found in the `repository <https://github.com/erikglee/hbcd_qmri_postproc>`_,
or it can be pulled from DockerHub as a singularity using the following command: ::
    
        singularity pull docker://dcanumn/hbcd_qmri_postproc:<version_num>

Where version_num denotes the specific version of the container. All available
versions of the container can be found `here <https://hub.docker.com/r/dcanumn/hbcd_qmri_postproc/tags>`_.

After downloading the container, singularity is the only other dependency needed
for processing. The full usage details can be seen under the :ref:`usage` section, but
the basic command to run the container is as follows: ::
    
        container_path=/path/to/container.sif
        bids_dir=/path/to/bids
        output_dir=/path/to/output
        qmri_dir=/path/to/qmri
        bibsnet_dir=/path/to/bibsnet
        singularity run -B $bids_dir:/bids \
         -B $output_dir:/output \
         -B $qmri_dir:/qmri \
         -B $bibsnet_dir:/bibsnet \
         $container_path /data /output participant /qmri /bibsnet

Where "singularity run" is first followed by specific commands for singularity.
In this case it is a series of "bind" commands that will give singularity
access to the necessary directories needed for processing. This is followed
by the path to the container and then the arguments for the primary script
to be ran by the container.