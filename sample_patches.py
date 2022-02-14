#!/usr/bin/env python3

import random
import sys

from pyvips import Image, Region

n_patches = 100
patch_size = 512
out_quality = 90


def main(filename: str):
    image = Image.new_from_file(filename, access="random")
    print("Opened image file", filename)
    # print("Image metadata:")
    # for field in image.get_fields():
    #     print(f"{field}: {image.get(field)}")

    if image.get("n-pages") != 6:
        raise RuntimeError("Unexpected image format.  Expected the image to have 6 pages")

    # the OME XML data is in the 'image-description' field of the first page if the main image
    # (i.e., page 0) and also in the first page of the smaller embedded image (i.e., page 3)

    # extract patches from the main image
    pages = [Image.new_from_file(filename, access="random", page=i) for i in range(3)]
    regions = [Region.new(p) for p in pages]

    def extract_and_write_patch(xcoord: int, ycoord: int, image_num: int):
        patches = [r.fetch(xcoord, ycoord, patch_size, patch_size) for r in regions]
        patch_images = [Image.new_from_memory(b, patch_size, patch_size, pages[0].bands, pages[0].format)
                        for b in patches]
        color = patch_images[0].bandjoin(patch_images[1:])
        color = color.copy(interpretation="multiband")
        out_filename = "{:03d}_{}_{}.jp2".format(image_num, xcoord, ycoord)
        color.write_to_file(out_filename, Q=out_quality, lossless=False)

    for i in range(n_patches):
        x = random.randint(0, image.width - patch_size)
        y = random.randint(0, image.height - patch_size)
        extract_and_write_patch(x, y, i)


if __name__ == "__main__":
    main(sys.argv[1])
