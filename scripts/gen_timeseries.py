#!/usr/bin/env python3

"""
script: generate time series for all input files in a direcory
"""

#++++++++++++++++++++++++++++++
# Import python modules
#++++++++++++++++++++++++++++++

import os
import logging
import sys
import glob
import argparse
import logging

# Determine local directory path:
_LOCAL_PATH = os.path.dirname(os.path.abspath(__file__))

from pathlib import Path

# Time series generation
from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection

# Dask
from dask.distributed import LocalCluster, client
from dask.distributed import wait, as_completed
from dask import delayed

from dask_jobqueue import SLURMCluster

#++++++++++++++++++++++++++++++
# Input argument parser function
#++++++++++++++++++++++++++++++

def parse_arguments():

    """
    Parses command-line input arguments using the argparse
    python module and outputs the final argument object.
    """

    #Create parser object:
    parser = argparse.ArgumentParser(description='Utility to create time series for all time slice files in a directory')

    parser.add_argument('--debug', action='store_true',
                        help="Turn on debug output (False by default).")

    parser.add_argument('--inputdir', type=str,
                        help="Comma separated full pathnames of directories containing input spectral element data files (required)",
                        required=True
                        )
    parser.add_argument("--realm",
                        choices=["atmos","land"],
                        help="Realm to process - sets include patterns for time series (required)",
                        required=True
                        )
    parser.add_argument('--outputdir', type=str,
                        help="Full path to directory where output time series data will be placed (optional) \n"
                        "if not specified will be put in inputdir/../time_series)",
                        )
    parser.add_argument("--overwrite_timeseries",
                        action="store_true",
                        help="Overwrite existing timeseries outputs (default: False)",
                        )
    parser.add_argument("--year_first",
                        type=int,
                        help="first year of history files to use",
                        ),
    parser.add_argument("--year_last",
                        type=int,
                        help="last year of history files to use",
                        ),
    parser.add_argument("--year_inc",
                        type=int,
                        help="how many years to use for each time series file",
                        ),
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
# main time series script
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
    logger = logging.getLogger("gen_timseries")

    # Set up dask if appropriate
    if args.workers == 1:
        client = None
        cluster = None
    else:
        cluster = LocalCluster(
            n_workers=args.workers, threads_per_worker=1, memory_limit="4GB",
        )
        client = cluster.get_client()

    # For each file in list of files - regrid data
    debug = args.debug

    # Determine include patterns
    if args.realm == "atmos":
        include_patterns = ["*cam.h0a*"]
        frequency = "mon"
    elif args.realm == "land":
        include_patterns = ["*clm2.h0a*"]
        frequency = "mon"

    # Determine input directory
    inputdir = Path(args.inputdir)

    # Determine output directories
    if args.outputdir:
        outputdir = Path(args.outputdir)
    else:
        outputdir = inputdir / '..' / 'time_series'

    # Create time series by default
    logger.info(f"Timeseries generation starting for files in {inputdir}...")
    logger.info(f"  output will be placed in {outputdir}...")

    # Determine number of files used in time series creation
    cnt = 0
    for include_pattern in include_patterns:
        cnt = cnt + len(glob.glob(os.path.join(inputdir, include_pattern)))
    if cnt == 0:
        logger.warning(f"No input files to process in {inputdir} with {include_patterns}")
        sys.exit(0)

    year_first = args.year_first
    year_last = args.year_last
    nstep = args.year_inc

    # Create base HFCollection
    hf_collection = HFCollection(inputdir, dask_client=client)

    for include_pattern in include_patterns:
        logger.info("Processing files with pattern: %s", include_pattern)

        for year in range(year_first, year_last+1, nstep):
            logger.info(f"Processing from year {year} to year {year+nstep-1}")
            hfp_collection = hf_collection.include_patterns([include_pattern])
            hfp_collection = hfp_collection.include_years(year, year+nstep-1)

            logger.info(f"files to process for year {year} are") 
            for item in list(hfp_collection):
                logger.info(f"{item}")

            # Reads metadata from all files matching this pattern
            # Gets variable names, dimensions, time information, etc.
            hfp_collection.pull_metadata()

            # Set up the time series generation for this pattern's files
            logger.info("Calling ts_collection")
            ts_collection = TSCollection(
                hfp_collection, outputdir, ts_orders=None, dask_client=client
            )
            logger.info("Finished ts_collection")

            # Apply overwrite if requested:
            # If --overwrite flag was passed, tells GenTS to overwrite existing time series files
            if args.overwrite_timeseries:
                ts_collection = ts_collection.apply_overwrite("*")

            # Perform the time series generation for this pattern
            ts_collection.execute()
            logger.info("Timeseries processing complete")

    if client:
        client.close()
    if cluster:
        cluster.close()

if __name__ == "__main__":
    main()
