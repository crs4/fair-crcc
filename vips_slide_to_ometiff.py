#!/usr/bin/env python3


# Authors: Luca Pireddu and Martin Kačenga

import argparse
import os
import sys
import time
import xml.etree.ElementTree as ET

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Mapping

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


def print_progress(progress) -> None:
    print(f"percent = {progress.percent}%; run time = {progress.run}s; ETA = {progress.eta}s", file=sys.stderr)
    if progress.percent == 100:
        print("Progress is 100%, but it'll take a bit longer it finish up.", file=sys.stderr)


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

    parser.add_argument('-p', '--pyramid', action='store_true', help="Generate pyramid")
    parser.add_argument('-t', '--tile-size', type=int, default=512)
    parser.add_argument('-v', '--verbose', action='store_true',
                        help="Be more verbose. Prints progress information from libvips")

    opts = parser.parse_args(args)

    return opts


ome_xml_template = """<?xml version="1.0" encoding="UTF-8"?>
<OME xmlns="http://www.openmicroscopy.org/Schemas/OME/2016-06"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.openmicroscopy.org/Schemas/OME/2016-06 http://www.openmicroscopy.org/Schemas/OME/2016-06/ome.xsd">
    <Image ID="Image:0">
        <Pixels ID="Pixels:0"
                DimensionOrder="XYCZT"
                Interleaved="false"
                SizeC="{image_bands}"
                SizeX="{image_width}"
                SizeY="{image_height}"
                SizeZ="1"
                SizeT="1"
                SignificantBits="8"
                Type="uint8">
         <Channel ID="Channel:0:0" SamplesPerPixel="3">
            <LightPath/>
         </Channel>
         <TiffData/>
        </Pixels>
    </Image>
</OME>"""


def create_ome_xml(image: pyvips.Image) -> str:
    xml_fields = dict(
        image_width=image.width,
        image_height=image.height,
        image_bands=3)  # RGB (original image loaded by openslide also includes alpha channel, so it has 4)
    initial_xml = ome_xml_template.format(**xml_fields)

    metadata = extract_metadata(image)

    ET.register_namespace("OME", "http://www.openmicroscopy.org/Schemas/OME/2016-06")
    root = ET.fromstring(initial_xml)
    structured_annotations = ET.SubElement(root, "OME:StructuredAnnotations")
    counter = 0
    for key, value in metadata.items():
        xml_annotation = ET.SubElement(structured_annotations, "OME:XMLAnnotation")
        xml_annotation.set("ID", "Annotation:" + str(counter))
        counter += 1
        val = ET.SubElement(xml_annotation, "OME:Value")
        original_metadata = ET.SubElement(val, "OriginalMetadata")
        k = ET.SubElement(original_metadata, "Key")
        k.text = key
        v = ET.SubElement(original_metadata, "Value")
        v.text = value
    return ET.tostring(root, encoding='utf-8')


def extract_metadata(image: pyvips.Image) -> Mapping[str, Any]:
    image_format = image.get('openslide.vendor')
    if image_format == "mirax":
        metadata = {f.rsplit('.', 1)[1]: image.get(f)
                    for f in image.get_fields() if f.startswith('mirax.GENERAL')}
    elif image_format == "aperio":
        metadata = {f.rsplit('.', 1)[1]: image.get(f)
                    for f in image.get_fields() if f.startswith('aperio')}
    elif image_format == "hamamatsu":
        metadata = {f.split('.', 1)[1]: image.get(f)
                    for f in image.get_fields() if f.startswith('hamamatsu')}

    return metadata


def main(args=None):
    """
    openslide to OME(?)-TIFF conversion using pyvips.
    """
    opts = parse_args(args)

    image = Image.openslideload(str(opts.original), access="sequential")

    # openslide will add an alpha ... drop it
    if image.hasalpha():
        image = image[:-1]

    # set minimal OME metadata
    image.set_type(pyvips.GValue.gstr_type, "image-description", create_ome_xml(image))
    image.set_type(pyvips.GValue.gint_type, "page-height", image.height)

    output_args = {
        'bigtiff': True,
        'tile': True,
        'tile_width': opts.tile_size,
        'tile_height': opts.tile_size,
        'properties': False,
        'compression': opts.compression,
        'pyramid': False,
        'subifd': False,
    }

    if opts.pyramid:
        output_args['pyramid'] = True
        output_args['depth'] = 'onetile'
        # subifd is required for OME-TIFF style pyramids, where the pyramid layers
        # are stored in the same page as the original image
        output_args['subifd'] = True

    if opts.quality:
        output_args['Q'] = opts.quality

    if opts.verbose:
        os.environ['VIPS_PROGRESS'] = "1"
        image.set_progress(True)

        last_progress_update = None

        def eval_callback(_, progress):
            nonlocal last_progress_update
            if last_progress_update != progress.percent:
                last_progress_update = progress.percent
                print_progress(progress)
        image.signal_connect('eval', eval_callback)

    print("Writing the tiff.  Arguments:", output_args, file=sys.stderr)
    image.tiffsave(str(opts.output), **output_args)


if __name__ == "__main__":
    with time_block():
        main(sys.argv[1:])
