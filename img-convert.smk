
# Run as:
# snakemake --snakefile ./img-convert.smk --profile ./profile --configfile zenbanc_config.yml --use-singularity --cores --verbose
#
# The configuration must specify the input and output directory trees as
# `img_directory` and `output_storage`

configfile: "config.yml"

import atexit
import multiprocessing as mp
import os
import shutil
import sys
import tempfile
from os.path import join
from pathlib import Path
from typing import Union

ContainerMounts = {
    "input": "/input-storage/",
    "output": "/output-storage/",
}

## Utility functions

def log(*args) -> None:
    print(*args, file=sys.stderr)


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


def map_path_to_container(path: Union[str, Path]) -> str:
    if not path:
        raise ValueError("key path is not specified")
    if not workflow.use_singularity:
        # if we're not using singularity, we don't care about where things are.
        return str(path)
    # else, we're using singularity
    if not isinstance(path, Path):
        path = Path(path)
    if path.is_relative_to(config['img_directory']):
        return str(Path(ContainerMounts['input']) / path.relative_to(config['img_directory']))
    if path.is_relative_to(config['output_storage']):
        return str(Path(ContainerMounts['output']) / path.relative_to(config['output_storage']))
    raise ValueError(f"Location {path} is not supported. Place things "
                     "in one of the directories mounted by singularity")

def delete_temp_directory(tmp_dir: Union[str, Path]) -> None:
    log("Deleting temporary directory", tmp_dir)
    shutil.rmtree(tmp_dir, ignore_errors=True)


def setup_tmp_dir() -> None:
    """
    Creates a temporary directory for the workflow run.
    If `tmp_dir` is specified in the configuration, it will be used as
    the base path.  The temporary directory will be at a location mounted
    in any containers instantiated by the workflow.
    """
    base_path = config.get('tmp_dir', config['output_storage'])

    # Call map_path_to_container to check base_path.  It will raise
    # an error if path is outside container mounts
    map_path_to_container(base_path)

    os.makedirs(base_path, exist_ok=True)
    # with the specified directory, create a random subdirectory to avoid
    # conflicts with other runs that use the same configuration value.
    tmp_dir = tempfile.mkdtemp(prefix="img-convert-wf", dir=base_path)
    log("Created temporary directory", tmp_dir)
    os.environ['TMPDIR'] = tmp_dir
    atexit.register(delete_temp_directory, tmp_dir)


def get_container_tmp_dir() -> str:
    mapped_path = map_path_to_container(os.environ['TMPDIR'])
    return str(mapped_path)


def get_tmp_dir() -> str:
    return os.environ['TMPDIR']


###### Workflow start ######

os.makedirs(config['output_storage'], exist_ok=True)
setup_tmp_dir()
log("Switching working directory to output directory", config['output_storage'])
workdir: config['output_storage']

# Set container mounts points based on the configuration provided
if workflow.use_singularity:
    workflow.singularity_args += " ".join([
        f" --bind {config['img_directory']}:{ContainerMounts['input']}:ro",
        f" --bind {config['output_storage']}:{ContainerMounts['output']}:rw",
        f" --pwd {ContainerMounts['output']}",
        f" --env TMPDIR={get_container_tmp_dir()}"])



shell.prefix("set -o pipefail; ")

source_slides = merge_globs(
    #glob_wildcards(config['img_directory'] + "/{relpath}/{slide,Ref09_000000000000B741|7395886177083037447|8151883121144003623}.mrxs"),
    glob_wildcards(config['img_directory'] + "/{relpath}/{slide}.mrxs"),
    glob_wildcards(config['img_directory'] + "/{relpath}/{slide}.svs"))

log("config:", config)
log("Use singularity?", workflow.use_singularity)
log("singularity_args:", workflow.singularity_args)
log("Source slides are: ", [ join(head, tail) for head, tail in zip(source_slides.relpath, source_slides.slide) ])


###### Input functions ######
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

##### Rules #####

rule all_tiffs:
    input:
        tiffs = expand("tiff_slides/{relpath}/{slide}.tiff", zip, relpath=source_slides.relpath, slide=source_slides.slide),
        checksums = "tiff_slides/tiff_checksums"


rule all_encrypted_tiffs:
    input:
        # 1. change tiff_slides/ paths to c4gh/ paths
        # 2. append suffixes .c4gh and .c4gh.sha
        encrypted_tiffs = 
            lambda _: [ str(Path("c4gh") / p.relative_to("tiff_slides").with_suffix(p.suffix + new_suffix))
                          for p in map(Path, rules.all_tiffs.input.tiffs)
                          for new_suffix in ('.c4gh', '.c4gh.sha')
                        ]


rule merge_tiff_checksums:
    input:
        expand("tiff_slides/{relpath}/{slide}.tiff.sha", zip, relpath=source_slides.relpath, slide=source_slides.slide)
    output:
        "tiff_slides/tiff_checksums"
    log:
        "tiff_slides/tiff_checksums.log"
    resources:
        mem_mb = 512
    container:
        "docker://ilveroluca/raw2ometiff:0.3.0"
    shell:
        """
        cat {input:q} | sort > {output:q} 2> {log:q}
        """


rule compute_tiff_checksum:
    input:
        tiff = "tiff_slides/{relpath}/{slide}.tiff"
    output:
        chksum = "tiff_slides/{relpath}/{slide}.tiff.sha"
    log:
        "tiff_slides/{relpath}/{slide}.tiff.sha.log"
    benchmark:
        "tiff_slides/{relpath}/{slide}.tiff.sha.bench"
    params:
        checksum_alg = 256
    resources:
        mem_mb = 64
    container:
        "docker://ilveroluca/raw2ometiff:0.3.0"
    shell:
        """
        sha{params.checksum_alg}sum {input:q} > {output:q} 2> {log:q}
        """


rule crypt_tiff:
    input:
        tiff = "tiff_slides/{relpath}/{slide}.tiff"
    output:
        crypt = protected("c4gh/{relpath}/{slide}.tiff.c4gh"),
        checksum = "c4gh/{relpath}/{slide}.tiff.c4gh.sha"
    log:
        "c4gh/{relpath}/{slide}.log"
    benchmark:
        "c4gh/{relpath}/{slide}.bench"
    params:
        checksum_alg = 256,
        private_key = lambda _: map_path_to_container(config['keypair']['private']),
        public_key = lambda _: map_path_to_container(config['keypair']['public'])
    resources:
        mem_mb = 1024, # guessed and probably overestimated
        disk_mb = lambda _, input: input.size
    container:
        "docker://ilveroluca/crypt4gh:1.5"
    shell:
        """
        mkdir -p $(dirname {output.crypt}) $(dirname {output.checksum}) &&
        crypt4gh encrypt --sk {params.private_key:q} --recipient_pk {params.public_key:q} < {input.tiff:q} > {output.crypt:q} 2> {log} &&
        sha{params.checksum_alg}sum {output.crypt:q} > {output.checksum:q} 2>> {log}
        """


rule mirax_to_raw:
    input:
        mrxs=lambda wildcard: gen_rule_input_path("mrxs", wildcard)
    output:
        directory(temp("raw_slides/{relpath}/{slide}.raw"))
    log:
        "raw_slides/{relpath}/{slide}.log"
    benchmark:
        "raw_slides/{relpath}/{slide}.bench"
    params:
        job_in = lambda _, input: convert_input_path_for_job(input),
        log_level = config.get('log_level', 'WARN'),
        max_cached_tiles = config.get('tiff', {}).get('max_cached_tiles', 64),
        #memo_directory = lambda _, output: str(Path(output[0]).parent),
        memo_directory = lambda _: get_container_tmp_dir(),
        tile_height = config.get('tiff', {}).get('tile_height', 1024),
        tile_width = config.get('tiff', {}).get('tile_width', 1024)
    container:
        "docker://ilveroluca/bioformats2raw:0.3.1"
    resources:
        mem_mb = 5000,
        disk_mb = lambda _, input: input.size * 15,
        tmpdir = lambda _: get_tmp_dir()
    threads:
        #lambda cores: max(1, mp.cpu_count() - 1)
        3
    shell:
        """
        mkdir -p $(dirname {output}) &&
        bioformats2raw \
            --log-level={params.log_level} \
            --max_workers=$((2 * {threads})) \
            --tile_height={params.tile_height} \
            --tile_width={params.tile_width} \
            --memo-directory={params.memo_directory} \
            --max_cached_tiles={params.max_cached_tiles} \
            {params.job_in:q} {output:q} &> {log}
        """


use rule mirax_to_raw as svs_to_raw with:
    input:
        svs=lambda wildcard: gen_rule_input_path("svs", wildcard)


rule raw_to_ometiff:
    priority: 10  # higher than other rules
    input:
        "raw_slides/{relpath}/{slide}.raw"
    output:
        protected("tiff_slides/{relpath}/{slide}.tiff")
    log:
        "tiff_slides/{relpath}/{slide}.log"
    benchmark:
        "tiff_slides/{relpath}/{slide}.bench"
    params:
        compression = config.get('tiff', {}).get('compression', 'JPEG'),
        quality = config.get('tiff', {}).get('quality', 80),
        log_level = config.get('log_level', 'WARN')
    container:
        "docker://ilveroluca/raw2ometiff:0.3.0"
    resources:
        mem_mb = 3000,
        disk_mb = lambda _, input: input.size / 10,
        tmpdir = lambda _: get_tmp_dir()
    threads:
        5
    shell:
        """
        mkdir -p $(dirname {output}) &&
        raw2ometiff \
            --compression={params.compression:q} \
            --quality={params.quality} \
            --log-level={params.log_level} \
            --max_workers={threads} \
            {input:q} {output:q} &> {log}
        """
