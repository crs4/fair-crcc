#!/usr/bin/env python3

import argparse
import sys
import time

from contextlib import contextmanager
from pathlib import Path

from pyvips import Image


@contextmanager
def time_block():
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        print("Running time: {:02f}".format(end_time - start_time), file=sys.stderr)


# The environment variable VIPS_CONCURRENCY should set the number of threads
# to be used by the VIPS library.  If it's not set, by default it should create
# as many threads as there are cores on the host.


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description="Create a thumbnail of a FAIR CRCC tiff image.")

    parser.add_argument("original", type=Path, metavar="IMAGE")
    parser.add_argument('-s', '--size', type=int, default=512,
                        help="Horizontal size of the resulting image")
    parser.add_argument('-o', '--output', type=Path,
                        help="Output file. By default prepends 'th_' to the original image name")

    opts = parser.parse_args(args)
    if not opts.output:
        opts.output = opts.original.parent / ("th_" + opts.original.name)

    return opts


def main(args=None):
    opts = parse_args(args)

    image = Image.new_from_file(str(opts.original))
    if image.get("n-pages") != 6:
        raise RuntimeError("Unexpected image format.  Expected the image to have 6 pages")

    pages = [Image.new_from_file(str(opts.original), page=i) for i in range(3)]
    color = pages[0].bandjoin(pages[1:])
    color = color.copy(interpretation="multiband")
    scale_factor = opts.size / color.width
    thumb = color.resize(scale_factor, kernel="linear")
    thumb.write_to_file(str(opts.output))


if __name__ == "__main__":
    with time_block():
        main(sys.argv[1:])
