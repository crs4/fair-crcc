
# == Options that are probably useful ==
#
# keep-going: true
# shadow-prefix: /mnt/rbd/data/sftp/fair-crcc/
# delete-temp-output: true


configfile: "config.yml"

from os.path import join
from pathlib import Path


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
def gen_input_path(suffix, wildcard):
    if not suffix:
        raise ValueError("suffix is not defined")
    path = Path(config['img_directory']) / f"{wildcard.relpath}/{wildcard.slide}.{suffix}"
    return str(path) if path.exists() else ""


rule all_tiffs:
    input:
        expand("tiff_slides/{relpath}/{slide}.tiff", zip, relpath=source_slides.relpath, slide=source_slides.slide)


rule mirax_to_raw:
    input:
        mrxs=lambda wildcard: gen_input_path("mrxs", wildcard)
    output:
        directory(temp("raw_slides/{relpath}/{slide}.raw"))
    log:
        "raw_slides/{relpath}/{slide}.log"
    container:
        "docker://ilveroluca/bioformats2raw:0.3.1"
    script:
        "--max_workers={threads} {input} {output}"


use rule mirax_to_raw as svs_to_raw with:
    input:
        svs=lambda wildcard: gen_input_path("svs", wildcard)


rule raw_to_ometiff:
    input:
        "raw_slides/{relpath}/{slide}.raw"
    output:
        protected("tiff_slides/{relpath}/{slide}.tiff")
    container:
        "docker://ilveroluca/raw2ometiff:0.3.0"
    script:
        " --max-workers={threads} {input} {output}"
