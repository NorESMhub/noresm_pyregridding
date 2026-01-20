#!/usr/bin/env python3

"""
script: regrid_all_files_in_folder
regrids all cam or clm output files from spectral element grid to output lat/lon grid 
"""

#++++++++++++++++++++++++++++++
# Import python modules
#++++++++++++++++++++++++++++++

import os
import logging
import sys
import glob
import argparse
import xarray as xr
import logging

# Determine local directory path:
_LOCAL_PATH = os.path.dirname(os.path.abspath(__file__))

from pathlib import Path

# Append path to regridding utilities
sys.path.append(os.path.join(_LOCAL_PATH, "../", "src"))

# Now import regridding utilities
from noresm_pyregridding import noresm_pyregridding

# Dask
from dask.distributed import LocalCluster
from dask.distributed import wait, as_completed
from dask import delayed

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
                        help="Turn on debug output (False by default).")

    parser.add_argument("--realm",
                        choices=["atm","lnd"],
                        help="Realm to process (required)",
                        required=True,)

    parser.add_argument('--inputdir', type=str,
                        help="Full pathname of directory containing input spectral element data files (required)",
                        required=True)

    parser.add_argument('--outputdir', type=str,
                        help="Full path to directory where output regridded data will be placed (required)",
                        required=True)

    parser.add_argument ('--inputres', type=str,
                         choices=["ne16","ne30"],
                         help="input_grid name (required)",
                         required=True)

    parser.add_argument("--workers",
                        type=int,
                        default=1,
                        help="Number of Dask workers (default: 1, set to >1 for parallel execution)",
                        )

    # Parse Argument inputs
    args = parser.parse_args()

    # Error checks
    return args

#++++++++++++++++++++++++++++++
# main regridding script
#++++++++++++++++++++++++++++++

def main():

    # Parse command-line arguments
    args = parse_arguments()

    # Set up logging
    if args.debug:
        logging.basicConfig(
            level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
        )
    else:
        logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logger = logging.getLogger("noresm_pyregridding")

    # Set up dask if appropriate
    if args.workers == 1:
        client = None
        cluster = None
    else:
        ncpus_env = os.getenv("NCPUS")
        if ncpus_env is not None:
            ml = 1.0 - float(int(ncpus_env) - 1) / 128.0
        else:
            ml = "auto"  # Default memory limit if NCPUS is not set
        cluster = LocalCluster(
            n_workers=args.workers, threads_per_worker=1, memory_limit=ml
        )
        client = cluster.get_client()

    # Determine weights file to use for regridding (all conservative for now)
    if (args.inputres == 'ne16'):
        weight_file = "/datalake/NS9560K/diagnostics/land_xesmf_diag_data/map_ne16pg3_to_1.9x2.5_nomask_scripgrids_c250425.nc"
    elif (args.inputres == 'ne30'): 
        weight_file = "/datalake/NS9560K/diagnostics/land_xesmf_diag_data/map_ne30pg3_to_0.5x0.5_nomask_aave_da_c180515.nc"
    else:
        raise Exception("only input grids of ne16 and ne30 are currently supported")

    # Create conservative regridder - want to only do this once
    logger.info(f"Creating conservative regridder")
    regridder = noresm_pyregridding.make_se_regridder(weight_file=weight_file)
    logger.info(f"successfully called regridder")

    # For each file in list of files - regrid data
    debug = args.debug

    # Determine input directory
    inputdir = Path(args.inputdir)

    # Determine output directories
    outputdir = Path(args.outputdir)
    if not os.path.exists(outputdir):
        outputdir.mkdir(parents=True, exist_ok=True)

    # Check that inputdir exists
    if not os.path.exists(inputdir):
        raise ValueError(f"inputdir {inputdir} does not exist")

    # Determine list of files to regrid
    filelist = glob.glob(f"{inputdir}/*.nc")
    if len(filelist) < 1:
        logger.error(f"No netcdf files found in {inputdir}")
        logger.debug(f"filelist is {filelist}")
        for filepath in filelist:
            filename = filepath.split("/")[-1]
            logger.info(f" filename is {filename}") 

    # Loop over files in inut directory
    for filepath in filelist:
        # Find filename and output filename and check if file has already been regridded
        filename = filepath.split("/")[-1]

        # Determine output file
        output_file = filename.replace(".nc", "_regridded.nc")
        outfile_exists = os.path.exists(os.path.join(outputdir,output_file))
        if outfile_exists:
            logger.info(f"Output file {output_file} already exists - skipping regridding for input {filepath}")
            continue

        # Regrid file
        logger.info(f"Regridding file {filepath}")
        data_in = xr.open_dataset(filepath)

        if not "ncol" in data_in.dims and not "lndgrid" in data_in.dims:
            raise ValueError("Neither ncol or lndgrid are on input data")

        if args.realm == 'atm':
            data_regridded = noresm_pyregridding.regrid_cam_se_data(regridder, data_in, debug)
        elif args.realm == 'lnd':
            data_regridded = noresm_pyregridding.regrid_ctsm_se_data(regridder, data_in, debug)
        logger.info(f"Successfully regridded file {filename}")

        # Write  out regridded file
        output_file = os.path.join(outputdir,output_file)
        data_regridded.to_netcdf(output_file)
        logger.info(f"Wrote regridded file {output_file}")
    
    if client:
        client.close()
    if cluster:
        cluster.close()

if __name__ == "__main__":
    main()
