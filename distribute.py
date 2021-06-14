#!/usr/bin/env python
"""
Easily split and distribute work for a set of
sample files across the cluster by automatically
generating appropriate job scheduler scripts 
(e.g. PBS,SLURM).

Given a template file and a list of sample IDs,
discover and set the parameters (either by user input
or by parameter file). The script will then split the
list of sample IDs into a user-specified number of
subsets and write out one script for each.
"""
import argparse
from itertools import islice
import os
import os.path as osp
import re
import sys

class ParametersException(Exception):
    """Exception raised for missing template parameters.

    Attributes:
        parameters -- the list of missing parameters
    """

    def __init__(self, missing_params):
        self.missing_params = missing_params
        self.message = "ERROR. No values found for the following parameters:\n"
        self.message += "\n".join(["  " + p for p in self.missing_params])

def verify_path(path, create=False):
    """
    Check that a given path exists. If create
    is specified (assumes path is a directory),
    attempt to issue os.mkdir to create the path.
    """
    if not osp.exists(path):
        if create:
            try:
                os.mkdir(path)
            except Exception as e:
                print(f"ERROR. Unable to create directory: {path}", 
                      file=sys.stderr)
                print(e, file=sys.stderr)
                return False
        else:
            print(f"ERROR. The specified path does not exist: {path}", 
                  file=sys.stderr)
            return False
    return True


def split(items, partitions):
    """
    Divide a collection into multiple partitions.
    If elements do not split equally, the last
    partition will have fewer elements.
    """
    split = []

    if partitions > 0:
        for i in range(0, len(items), partitions):
            split.append(items[i: i+partitions])
    return split


def parse_parameters_file(path, params):
    """
    Read in parameters from an input file. If the parameters 
    found in the supplied file do not match the set of parameters 
    contained in the template file (params), then and raise
    a ParametersException containing a list 
    """
    file_params = {}
    with open(path) as inf:
        for line in inf:
            p, v = [item.strip() for item in line.split(":", maxsplit=1)]
            file_params[p] = v
    
    missing = params.difference(file_params)
    if missing.difference({'job_id', 'samples_fp'}):
        raise ParametersException(missing)

    return file_params

def input_params(params):
    """
    Given a set of discovered parameters, ask for user-input
    to define each one.
    """
    print(f"The following {len(params)} parameters were found "
           "in the template file.")
    print("Please enter values for each:")
    param_vals = {}
    for param in params:
        if param not in ["samples_fp", "job_id"]:
            param_vals[param] = input(f"{param}: ")

    return param_vals



def handle_program_options():
    """Parses the given options passed in at the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("job_template",
                        help="Path to the templated job script. The output"
                        " files will be prefixed with the name of this file"
                        " up to the first underscore. For example, if the job"
                        " file is named: process_template.pbs, then the output"
                        " files will be named: process_0.pbs, process_1.pbs,"
                        " ..., etc. If no underscore is present, then the name"
                        " of the template file excluding the extension.")
    
    sid_me = parser.add_mutually_exclusive_group(required=True)
    sid_me.add_argument("-l", "--sample_ids_list_fp",
                        help="Path to the list of sample IDs.")
    sid_me.add_argument("-s", "--split_sample_ids_fps", nargs="+",
                        help="Paths to split sample ID files generated in a"
                             " previous run of this program.")
    
    parser.add_argument("-n", "--partition", type=int, default=10,
                        help="The number of samples to process in each job"
                        " script. Defaults to 10. NOTE: Will be ignored"
                        " if -s/--split_sample_ids_fps is specified.")
    parser.add_argument("-p", "--parameters_fp",
                        help="Path to a file containing values for each"
                        " parameter in the template file. Each pair should"
                        " occupy a separate line and have the format"
                        " parameter: value (surrounding spaces are ignored).")
    parser.add_argument("-o", "--output_dir", default=".",
                        help="The directory in which to place the output"
                        " files: subset sample IDs and job scripts. Defaults"
                        " to the current directory.")
    parser.add_argument("--save_params",
                        help="Save the recorded parameter values to this file.")

    return parser.parse_args()


def main():
    args = handle_program_options()

    ## Verify all input file paths
    path_errs = []
    path_errs.append(verify_path(args.job_template))

    #  Sample IDs list file
    if args.sample_ids_list_fp is not None:
        path_errs.append(verify_path(args.sample_ids_list_fp))
    #  Pre-split Sample ID files
    if args.split_sample_ids_fps is not None:
        for fp in args.split_sample_ids_fps:
            path_errs.append(verify_path(fp))
    # Template Parameters file
    if args.parameters_fp is not None:
        path_errs.append(verify_path(args.parameters_fp))
    # Output directory
    if args.output_dir != ".":
        path_errs.append(verify_path(args.output_dir, create=True))

    if not all(path_errs):
        sys.exit(1)


    ## Process template file and retrieve parameter values
    with open(args.job_template) as inf:
        template = inf.read()
    params = set(re.findall(r'\{(.*?)\}', template))

    if args.parameters_fp is None:
        param_vals = input_params(params)
    else:
        try:
            param_vals = parse_parameters_file(args.parameters_fp, params)
        except ParametersException as pe:
            sys.exit(pe.message)


    ## Split sample IDs or input pre-split ID files
    sample_groups = []

    if args.sample_ids_list_fp is not None:
        with open(args.sample_ids_list_fp) as sf:
            sample_groups = split(sf.read().splitlines(), args.partition)
    else:
        for fp in args.split_sample_ids_fps:
            sample_groups.append([fp])

    ## Output files. js = job script
    js_name, js_ext = osp.splitext(osp.basename(args.job_template))
    js_prefix = js_name.split("_")[0]
    for i, group in enumerate(sample_groups):
        i += 1

        if args.split_sample_ids_fps is not None:
            samples_fp = args.split_sample_ids_fps[i-1]
        else:
            samples_fp = osp.join(args.output_dir, f"{js_prefix}_samples_{i}.txt")
        js_fp = osp.join(args.output_dir, f"{js_prefix}_{i}{js_ext}")

        # Write the group sample IDs to disk
        if args.split_sample_ids_fps is None:
            with open(samples_fp, "w") as sf:
                sf.write("\n".join(group))
                sf.write("\n")

        # Write parameterized job scripts
        with open(js_fp, "w") as jsf:
            param_vals["samples_fp"] = samples_fp
            param_vals["job_id"] = str(i)
            jsf.write(template.format(**param_vals))

    if args.save_params is not None:
        skip_params = ["samples_fp", "job_id"]
        with open(args.save_params, "w") as outpf:
            outpf.write("\n".join([f"{p}: {v}" for p, v in param_vals.items() if p not in skip_params]))
            outpf.write("\n")

if __name__ == "__main__":
    sys.exit(main())
