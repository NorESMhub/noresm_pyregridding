#!/bin/env python

import os
import sys
import glob

import numpy as np
import xarray as xr

sys.path.append(os.path.join(os.path.dirname(__file__), "../", "src"))

from noresm_pyregridding import noresm_pyregridding

def print_help_message():
    print("Usage: python regrid_se_data.py <run_path>")
    print("<run_path> : Path to se output data, path to folder is expected!")
    sys.exit(1)

# Making sure there is a run_path argument
if len(sys.argv) < 2:
    print("You must supply a path to land output data, path to lnd/hist folder is expected!")
    print_help_message()
if sys.argv[1] == "--help":
    print_help_message()
run_path = sys.argv[1]
print (f"run_path is {run_path}")
if not os.path.exists(run_path):
    print("You must supply a path to land output data,  path to lnd/hist folder is expected!")
    print(f"path {run_path} does not exist")
    print_help_message()

weight = "/datalake/NS9560K/diagnostics/land_xesmf_diag_data/map_ne16pg3_to_1.9x2.5_nomask_scripgrids_c250425.nc"

# TODO: determine mapping type (conservative versus bilinear)

# Determine dimension name (currently lndgrid for clm and ncols for atm)
#dimname = 'ncols'
dimname = 'lndgrid'

# Create regridder
print (f"Creating regridder for conservative maping")
regridder = noresm_pyregridding.make_se_regridder(weight_file=weight)
print (f"successfully called regridder")
outpath = "out_dump"

# Determine list of files to regrid
filelist = glob.glob(f"{run_path}*.nc")
print (f"filelist is {filelist}")
if len(filelist) < 1:
    print(f"No netcdf files found in {run_path}")
    print_help_message()

# For each file in list of files - regridd data
for filepath in filelist:

    # Regrid file
    print(f"Regridding file {filepath}")
    data = xr.open_dataset(filepath)
    data_regridded = noresm_pyregridding.regrid_se_data(regridder, data, dimname)
    print(f"Successfully regridded file {filepath}")

    # Write  out regridded file
    filename = filepath.split("/")[-1]
    output_file = os.path.join(outpath, filename.replace(".nc", "_regridded.nc"))
    data_regridded.to_netcdf(output_file)
    print(f"Wrote regridded file {output_file}")
    sys.exit(4)
