.. HBCD_CBRAIN_PROCESSING documentation master file, created by
   sphinx-quickstart on Wed Jun  5 10:48:12 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Outputs
=======

The application outputs will generally mirror the structure
of the inputs. The overall output folder structure can be seen
below, with brackets denoting optional terms that are present if
the input: ::

    <output_dir>/sub-<label>[/ses-<label>]/anat/

Within this folder, the following (at minimum) can be found: ::
    

    (a) sub-<label>[_ses-<label>]_desc-RegistrationQCAid.png
    (b) sub-<label>[_ses-<label>]_desc-RegistrationQCAid.json
    (c) sub-<label>[_ses-<label>]_desc-space-<label>_desc-aseg_dseg.nii.gz
    (d) sub-<label>[_ses-<label>]_desc-AsegROIs_scalarstats.tsv
    (e) sub-<label>[_ses-<label>]_desc-AsegROIs_scalarstats.json
    (f) sub-<label>[_ses-<label>]_space-<label>_desc-<label>_<label>.nii.gz
    (g) sub-<label>[_ses-<label>]_space-<label>_desc-<label>_<label>.json

(a,b) Are used to visualize the registration between the quantitative
images of interest, and the high resolution anatomical space where the
segmentation was originally found. Depending on what relaxometry maps are
available, either T2, T1, or PD maps will be used as an underlay with 
a purple outline showing the outer boundary of the brain (taken from
the segmentation image).

(c) Is a copy of the segmentation that has been registered and resampled to
the space of the quantitative images using nearest neighbor interpolation.

(d, e) Are created by applying the voxel labels from (c) to any of the
(at most) the T1/T2/PD maps in the input qMRI directory. Output statistics
include 1st percentile, median, mean, standard deviation, and 99th percentile
for each of the region labels found within (c), applied to all available maps.
Files with a similar format will also be created if a JSON is passed to the
region_groupings_json flag. In this case the "desc" label will contain the name
of the JSON (not including the extension), and the contents of the file will be
statistics that are calculated for any custom groupings of regions that are specified
in the JSON. If a region grouping is identified that does not have any associated
voxels in (c), then a n/a value for the associated statistics will be stored within
the tsv file. All the statistics within (d) are calculated using the segmentation ROIs
that have been resampled into the space of the quantitative maps.

(f, g) Is the result of using the inverse of the transform used for (c) to register
any available maps to the space of the input segmentation and high-resolution anatomical
from the BIDS input directory. The nifti images following the naming (f) have been resampled
using bSpline interpolation. At a minimum there will be one nifti/json pair here, with
up to 3 pairs if all T1/T2/PD maps are present.

.. toctree::
   :maxdepth: 2
   :caption: Contents: