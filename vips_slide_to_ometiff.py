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
    parser.add_argument('-v', '--verbose', action='store_true',
                        help="Be more verbose. Prints progress information from libvips")

    opts = parser.parse_args(args)

    return opts


ome_xml_template = """<?xml version="1.0" encoding="UTF-8"?>
<OME xmlns="http://www.openmicroscopy.org/Schemas/OME/2016-06"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.openmicroscopy.org/Schemas/OME/2016-06 http://www.openmicroscopy.org/Schemas/OME/2016-06/ome.xsd">
    <Image ID="Image:0">
        <!-- Minimum required fields about image dimensions -->
        <Pixels DimensionOrder="CXYZT"
                ID="Pixels:0"
                SizeC="{image_bands}"
                SizeX="{image_width}"
                SizeY="{image_height}"
                SizeZ="1"
                SizeT="1"
                Type="uint8">
            <MetadataOnly/>
        </Pixels>
    </Image>
</OME>"""


def create_ome_xml(image: pyvips.Image) -> str:
    xml_fields = dict(
        image_width=image.width,
        image_height=image.height,
        image_bands=image.bands)
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

    image = Image.openslideload(str(opts.original), access="sequential", attach_associated=True)

    # openslide will add an alpha ... drop it
    if image.hasalpha():
        image = image[:-1]

    # set minimal OME metadata
    image.set_type(pyvips.GValue.gstr_type, "image-description", create_ome_xml(image))

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
