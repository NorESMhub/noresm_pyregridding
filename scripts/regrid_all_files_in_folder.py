#!/usr/bin/env python3

"""
script: regrid_all_files_in_folder
regrids all cam or clm output files from spectral element grid to output lat/lon grid 
"""

#++++++++++++++++++++++++++++++
# Import python modules
#++++++++++++++++++++++++++++++

import os
import sys
import glob
import argparse
import xarray as xr

# Determine local directory path:
_LOCAL_PATH = os.path.dirname(os.path.abspath(__file__))

# Append path to regridding utilities
sys.path.append(os.path.join(_LOCAL_PATH, "../", "src"))

# Now import regridding utilities
from noresm_pyregridding import noresm_pyregridding

#++++++++++++++++++++++++++++++
# Input argument parser function
#++++++++++++++++++++++++++++++

def parse_arguments():

    """
    Parses command-line input arguments using the argparse
    python module and outputs the final argument object.
    """

    #Create parser object:
    parser = argparse.ArgumentParser(description='Command-line wrapper to regridder from SE to lat/lon grid')

    parser.add_argument('--debug', action='store_true',
                        help="Turn on debug output.")

    parser.add_argument('--indir',
                        help="Full path to directory containing input spectral element data files (required)",
                        required=True)

    parser.add_argument('--outdir',
                        help="Full path to directory where output regridded data will be placed (required)",
                        required=True)

    parser.add_argument ('--indim',
                         choices=("lndgrid","ncol"),
                         help="dimension name to regrid on input files (required)",
                         required=True)

    parser.add_argument ('--ingrid',
                         choices=("ne16","ne30"),
                         help="input_grid name (required)",
                         required=True)

    # Parse Argument inputs
    args = parser.parse_args()

    # Error checks
    if not os.path.exists(args.indir):
        errmsg = f"ERROR: input pathname {args.indir} does not exist"
        parser.error(errmsg)

    input_files = glob.glob(f"{args.indir}*.nc")
    if len(input_files) < 1:
        parser.error(f"No netcdf files found in {args.indir}")

    return args

#++++++++++++++++++++++++++++++
# main regridding script
#++++++++++++++++++++++++++++++

# Parse command-line arguments
args = parse_arguments()

# TODO: the following is only for area average - need to introduce more generality for bilinear mapping for
# intensive variables 

# Determine weights file to use for regridding
if (args.ingrid == 'ne16'):
    weight_file = "/datalake/NS9560K/diagnostics/land_xesmf_diag_data/map_ne16pg3_to_1.9x2.5_nomask_scripgrids_c250425.nc"
elif (args.ingrid == 'ne30'): 
    weight_file = "/datalake/NS9560K/diagnostics/land_xesmf_diag_data/map_ne30pg3_to_0.5x0.5_nomask_aave_da_c180515.nc"
else:
    raise Exception("only input grids of ne16 and ne30 are currently supported")

# Create regridder
print (f"Creating regridder for conservative maping")
regridder = noresm_pyregridding.make_se_regridder(weight_file=weight_file)
print (f"successfully called regridder")

# For each file in list of files - regridd data
input_dir = args.indir
output_dir = args.outdir
filelist = glob.glob(f"{input_dir}*.nc")
for filepath in filelist:

    # Regrid file
    print(f"Regridding file {filepath}")
    data = xr.open_dataset(filepath)
    dimname = args.indim
    print (f" using dimname of {dimname}")
    data_regridded = noresm_pyregridding.regrid_se_data(regridder, data, dimname)
    print(f"Successfully regridded file {filepath}")

    # Write  out regridded file
    filename = filepath.split("/")[-1]
    output_file = os.path.join(output_dir, filename.replace(".nc", "_regridded.nc"))
    data_regridded.to_netcdf(output_file)
    print(f"Wrote regridded file {output_file}")
    sys.exit(4)
