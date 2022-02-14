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


def print_progress(progress):
    print(f"run = {progress.run}, eta = {progress.eta}, "
          f"percent = {progress.percent} tpels = {progress.tpels}, "
          f"npels = {progress.npels}", file=sys.stderr)


def eval_cb(image, progress):
    print("eval:", file=sys.stderr)
    print_progress(progress)


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description="Convert OME-TIFF to BigTIFF")

    parser.add_argument("original", type=Path, metavar="SOURCE")
    parser.add_argument("output", type=Path, metavar="DEST")

    parser.add_argument('-c', '--compression', default='jpeg',
                        choices=("none", "jpeg", "deflate", "packbits", "ccittfax4", "lzw", "webp", "zstd", "jp2k"))
    parser.add_argument('-p', '--pyramid', action='store_true',
                        help="Generate pyramid")
    parser.add_argument('-t', '--tile-size', type=int, default=512)

    opts = parser.parse_args(args)

    return opts


def main(args=None):
    opts = parse_args(args)

    image = Image.new_from_file(str(opts.original), access="sequential")
    if image.get("n-pages") != 6:
        raise RuntimeError("Unexpected image format.  Expected the image to have 6 pages")

    pages = [Image.new_from_file(str(opts.original), access="sequential", page=i) for i in range(3)]

    color = pages[0].bandjoin(pages[1:])
    color = color.copy(interpretation="multiband")

    output_args = {
        'bigtiff': True,
        'tile': True,
        'tile_width': opts.tile_size,
        'tile_height': opts.tile_size,
        'properties': True,
        'compression': opts.compression,
        'pyramid': False,
    }

    if opts.pyramid:
        output_args['pyramid'] = True
        output_args['depth'] = 'onetile'

    color.set_progress(True)
    color.signal_connect('eval', eval_cb)

    print("Writing the tiff.  Arguments:", output_args, file=sys.stderr)
    color.tiffsave(str(opts.output), **output_args)


if __name__ == "__main__":
    with time_block():
        main(sys.argv[1:])
