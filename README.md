# noresm_pyregridding

An XESMF based diagnostic regridding tool to produce regular lat-lon grid files from raw spectral element output

Currently very much a work in progress

## Prerequisites

In order to use the tool you need to load and ESMF module and build an xesmf-containing conda environment on top of it. On Nird running, navigating to the folder called `scripts` and running:

```
. setup.sh
```
will automatically do this for you.

Otherwise you can install an environment using the file `conda_env.yaml` to install such an environment yourself using conda (or Miniforge). However, be aware that to do so you also need ESMF installed and loaded before building, and you need to loaded the same ESMF module when loading the environment. We recommend editing the `setup.sh` file to reflect the setup needed on your machine if you build your own environment this way.

## Usage

To run navigate to the folder called scripts (or extend paths for run-scripts to include the full path to that folder in the following commands) and run: 
```
. setup.sh
```

Then run 
```
python regrid_all_files_in_folder.py --indir raw_data_folder_path --outdir path_to_dump_output --component {cam, ctsm} --ingrid {ne16, ne30}
```
where `raw_data_folder_path` is the path to the raw output you want to regrid (typically lnd/hist or atm/hist folders), `path_to_dump_output` is the path to dump output, the regridding can be run for either the `cam` or the `ctsm` component and for either the `ne16` or the `ne30` resolution.

The regridder will attempt to regrid all data in each of the `.nc` files in the `raw_data_folder_path` and for each file, the regridded data will be contained in a file of the same name as the original file, but with an `_regridded.nc` filename ending in place of original `.nc` filename, hence you can in principle send the same folder for input as for output. Files that have been regridded before (i.e. a `_regridded.nc` file already exists in the `path_to_dump_output`) will be skipped for efficiency.

If the run usage is incorrect or you run the script as:

```
python regrid_all_files_in_folder.py -h
```
or
```
python regrid_all_files_in_folder.py --help
```
a short help message will be printed.