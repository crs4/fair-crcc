"""\
Build an RO-Crate for the workflow.
"""

import argparse
import os
from pathlib import Path

from rocrate.rocrate import ROCrate
from snakemake.workflow import Workflow


REPO_URL = "https://github.com/crs4/fair-crcc"


def get_wf_id(repo_dir):
    ids = [_.name for _ in os.scandir(repo_dir) if _.name.endswith(".smk")]
    if not ids:
        raise RuntimeError(".smk workflow file not found")
    return ids[0]


def parse_workflow(workflow_path):
    wf = Workflow(snakefile=workflow_path, overwrite_configfiles=[])
    try:
        wf.include(workflow_path)
    except Exception:
        pass
    wf.execute(dryrun=True, updated_files=[])
    return wf


def make_crate(repo_dir, out):
    wf_id = get_wf_id(repo_dir)
    crate = ROCrate(gen_preview=False)
    wf_source = Path(repo_dir) / wf_id
    wf_obj = parse_workflow(wf_source)
    # snakemake version required by the workflow?
    workflow = crate.add_workflow(wf_source, wf_id, main=True,
                                  lang="snakemake", gen_cwl=False)
    for relpath in wf_obj.configfiles:
        source = Path(repo_dir) / relpath
        crate.add_file(source, relpath, properties={
            "description": "workflow configuration file"
        })
    workflow["name"] = crate.root_dataset["name"] = wf_source.stem
    workflow["url"] = crate.root_dataset["isBasedOn"] = REPO_URL
    # workflow["version"] = ???
    # crate.root_dataset["license"] = ???
    readme_source = Path(repo_dir) / "README.md"
    if readme_source.is_file():
        crate.add_file(readme_source, "README.md")
    # suite = crate.add_test_suite(identifier="#test1")
    # resource = ???
    # crate.add_test_instance(suite, GH_API_URL, resource=resource,
    #                         service="github", identifier="test1_1")
    if out.endswith(".zip"):
        crate.write_zip(out)
    else:
        crate.write(out)


def main(args):
    make_crate(args.root, args.output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("root", metavar="ROOT_DIR", help="top-level directory")
    parser.add_argument("-o", "--output", metavar="DIR OR ZIP",
                        default="fair-crcc",
                        help="output RO-Crate directory or zip file")
    main(parser.parse_args())
