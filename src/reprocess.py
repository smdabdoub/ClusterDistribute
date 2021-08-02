#!/usr/bin/env python
# coding: utf-8
"""
Automatically collect samples from failed runs of a template job.
If a job writes output to the final folder before failure, the 
user can optionally specify the job output folder and a pattern
to match output files to check for samples that completed from a 
failed job.
"""
import argparse
from pathlib import Path
import re
import sys


def parse_log_files(log_files, fail_str):
    failed_runs = []
    
    for lf in log_files:
        with open(lf) as inf:
            if re.search(fail_str, inf.read()):
                failed_runs.append(lf.stem.split("_")[-1])
    print(f"Failed runs found: {len(failed_runs)}")
    return failed_runs


def gather_failed_samples(root, failed_runs, samples_pattern, job_folder, job_out_pattern):
    sample_files = root.glob(samples_pattern)
    failed_sample_files = sorted([f for f in sample_files if f.stem.split("_")[-1] in failed_runs])
    # all([a == b.stem.split("_")[-1] for a, b in zip(sorted(failed_runs), failed_sample_files)])
    
    sample_ids = set()
    for sfp in failed_sample_files:
        with open(sfp) as inf:
            sample_ids |= set(inf.read().splitlines())
    
    if job_folder:
        completed = {fp.stem.split('_')[0] for fp in job_folder.glob(job_out_pattern)}
    else:
        completed = set()
    
    print(f"Samples in failed jobs: {len(sample_ids)}")
    return sorted(sample_ids - completed)


def write_failed_samples(failed_samples, out_fp):
    with open(out_fp, 'w') as outf:
        outf.write('\n'.join(failed_samples))


def handle_program_options():
    """Parses the given options passed in at the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log_pattern",
                        help="File path pattern to locate job logs to determine"
                             " which jobs failed. Example: megahit*.out")
    parser.add_argument("fail_string",
                        help="A string to look for in the job logs that"
                             " indicates job failure.")
    parser.add_argument("samples_pattern",
                        help="File path pattern to locate sample ID"
                             " files that match the job logs. All the samples"
                             " from the failed jobs will be combined and "
                             " written out to be used with distribute.py."
                             " Example: 'megahit_F_samples*.txt'")

    parser.add_argument("-j", "--job_folder", default="",
                        help="Path to the folder containing completed jobs."
                             " Specify this and --job_out_pattern if the job"
                             " output folder contains completed samples from"
                             " partially completed jobs. Such samples will not"
                             " be included in the final failed samples output.")
    parser.add_argument("-p", "--job_out_pattern", default="",
                        help="File pattern to match job output files"
                             " (see --job_folder). Matched files will be parsed"
                             " to extract the sample ID. Assumes sample IDs are"
                             " at the beginning of the file name and followed"
                             " by an underscore (ex: S123_something.fna).")
    parser.add_argument("-o", "--failed_samples_fp", 
                        default="failed_samples.txt",
                        help="Path to the file that collects all the samples"
                             " from failed jobs. Default: failed_samples.txt")

    return parser.parse_args()


def main():
    args = handle_program_options()

    root = Path(args.log_pattern).parent
    lp = Path(args.log_pattern).name

    failed_runs = parse_log_files(root.glob(lp), args.fail_string)
    failed_samples = gather_failed_samples(root, failed_runs, 
                                           args.samples_pattern, 
                                           Path(args.job_folder), 
                                           args.job_out_pattern)
    print(f"Total failed samples: {len(failed_samples)}\n")
    
    write_failed_samples(failed_samples, args.failed_samples_fp)
    print(f"Failed samples written to: {args.failed_samples_fp}")



if __name__ == "__main__":
    sys.exit(main())
