
# == Options that are probably useful ==
#
# keep-going: true
# shadow-prefix: /mnt/rbd/data/sftp/fair-crcc/
# delete-temp-output: true


configfile: "config.yml"

from os.path import join
from pathlib import Path

ContainerMounts = {
    "input": "/input-storage/",
    "output": "/output-storage/",
}

# Set container mounts points based on the configuration provided
if workflow.use_singularity:
    workflow.singularity_args += f" --bind {config['img_directory']}:{ContainerMounts['input']}:ro"
    workflow.singularity_args += f" --bind {config['output_storage']}:{ContainerMounts['output']}:rw"
    workflow.singularity_args += f" --pwd {ContainerMounts['output']}"


## Utility functions
def merge_globs(*globs):
    """Concatenates compatible wildcards into a new object"""
    if len(globs) <= 0:
        return None
    if len(globs) == 1:
        return globs[0]
    if any(globs[0]._fields != g._fields for g in globs):
        raise ValueError("Wildcards have mismatching fields")

    fields = globs[0]._fields
    merged = {fname: [v for g in globs for v in getattr(g, fname)] for fname in fields}
    new_wildcard = globs[0].__class__(**merged)
    return new_wildcard



### Workflow start ###

shell.prefix("set -o pipefail; ")

source_slides = merge_globs(
    glob_wildcards(config['img_directory'] + "/{relpath}/{slide}.mrxs"),
    glob_wildcards(config['img_directory'] + "/{relpath}/{slide}.svs"))

print("Source slides are: ", [ join(head, tail) for head, tail in zip(source_slides.relpath, source_slides.slide) ])

## Input functions
def gen_rule_input_path(suffix, wildcard):
    if not suffix:
        raise ValueError("suffix is not defined")
    path = Path(config['img_directory']) / f"{wildcard.relpath}/{wildcard.slide}.{suffix}"
    return str(path) if path.exists() else ""


def convert_input_path_for_job(input_obj):
    """
    Converts an input path (formed by gen_rule_input_path) to a path
    suitable for the execution of the *_to_raw rules.  In particular,
    if we're using containers, we need to modify the path so that it's
    relative to the mount-point inside the container.
    """
    fs_path = input_obj[0]
    if workflow.use_singularity:
        job_input = Path(ContainerMounts['input']) / Path(fs_path).relative_to(config['img_directory'])
    else:
        job_input = fs_path
    return str(job_input)

## Rules

rule all_tiffs:
    input:
        expand("tiff_slides/{relpath}/{slide}.tiff", zip, relpath=source_slides.relpath, slide=source_slides.slide)


rule mirax_to_raw:
    input:
        mrxs=lambda wildcard: gen_rule_input_path("mrxs", wildcard)
    output:
        directory(temp("raw_slides/{relpath}/{slide}.raw"))
    log:
        "raw_slides/{relpath}/{slide}.log"
    params:
        job_in = lambda wildcard, input: convert_input_path_for_job(input)
    container:
        "docker://ilveroluca/bioformats2raw:0.3.1"
    threads:
        7
    shell:
        """
        mkdir -p $(dirname {output}) && bioformats2raw --max_workers={threads} {params.job_in} {output}
        """


use rule mirax_to_raw as svs_to_raw with:
    input:
        svs=lambda wildcard: gen_rule_input_path("svs", wildcard)


rule raw_to_ometiff:
    input:
        "raw_slides/{relpath}/{slide}.raw"
    output:
        protected("tiff_slides/{relpath}/{slide}.tiff")
    log:
        "tiff/{relpath}/{slide}.log"
    container:
        "docker://ilveroluca/raw2ometiff:0.3.0"
    threads:
        4
    shell:
        """
        mkdir -p $(dirname {output}) && raw2ometiff --max_workers={threads} {input} {output}
        """
