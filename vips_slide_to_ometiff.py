#!/usr/bin/env python3

import argparse
import os
import sys
import time

from contextlib import contextmanager
from pathlib import Path

import pyvips
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
        description="Convert from a supported openslide format to BigTIFF")

    parser.add_argument("original", type=Path, metavar="SOURCE")
    parser.add_argument("output", type=Path, metavar="DEST")

    parser.add_argument('-c', '--compression', default='jpeg',
                        choices=("none", "jpeg", "deflate", "packbits", "ccittfax4", "lzw", "webp", "zstd", "jp2k"))

    def quality_value(x):
        x = int(x)
        if x <= 0:
            raise argparse.ArgumentTypeError("Quality value must be > 0")
        return x
    parser.add_argument('-q', '--quality', type=quality_value)

    parser.add_argument('--no-pyramid', action='store_true',
                        help="Don't generate pyramid")
    parser.add_argument('-t', '--tile-size', type=int, default=512)

    opts = parser.parse_args(args)

    return opts


def create_image_description(image: pyvips.Image) -> str:
    template = """<?xml version="1.0" encoding="UTF-8"?>
        <OME xmlns="http://www.openmicroscopy.org/Schemas/OME/2016-06"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://www.openmicroscopy.org/Schemas/OME/2016-06 http://www.openmicroscopy.org/Schemas/OME/2016-06/ome.xsd">
            <Image ID="Image:0">
                <!-- Minimum required fields about image dimensions -->
                <Pixels DimensionOrder="XYCZT"
                        ID="Pixels:0"
                        SizeX="{image_width}"
                        SizeY="{image_height}"
                        SizeC="{image_bands}"
                        SizeZ="1"
                        SizeT="1"
                        Type="uint8">
                </Pixels>
            </Image>
        </OME>"""

    fields = dict(
        image_width=image.width,
        image_height=image.height,
        image_bands=image.bands)

    return template.format(**fields)


def main(args=None):
    """
    openslide to OME-TIFF conversion using pyvips.

    Based on example by John Cupitt
    (https://forum.image.sc/t/writing-qupath-bio-formats-compatible-pyramidal-image-with-libvips/51223/6)
    """
    opts = parse_args(args)

    image = Image.openslideload(str(opts.original), access="sequential", attach_associated=True)

    # openslide will add an alpha ... drop it
    if image.hasalpha():
        image = image[:-1]

    # split to separate image planes and stack vertically ready for OME
    image = Image.arrayjoin(image.bandsplit(), across=1)

    # set minimal OME metadata
    # before we can modify an image (set metadata in this case), we must take a
    # private copy
    image = image.copy()
    image.set_type(pyvips.GValue.gint_type, "page-height", image.height)
    image.set_type(pyvips.GValue.gstr_type, "image-description", create_image_description(image))

    output_args = {
        'bigtiff': True,
        'tile': True,
        'tile_width': opts.tile_size,
        'tile_height': opts.tile_size,
        'properties': True,
        'compression': opts.compression,
        'subifd': True,
        'pyramid': False,
    }

    if not opts.no_pyramid:
        output_args['pyramid'] = True
        output_args['depth'] = 'onetile'

    if opts.quality:
        output_args['Q'] = opts.quality

    image.set_progress(True)
    image.signal_connect('eval', eval_cb)

    print("Writing the tiff.  Arguments:", output_args, file=sys.stderr)
    image.tiffsave(str(opts.output), **output_args)


if __name__ == "__main__":
    os.environ['VIPS_PROGRESS'] = "1"
    with time_block():
        main(sys.argv[1:])
