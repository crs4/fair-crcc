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
    print("Image metadata:")
    for field in image.get_fields():
        print(f"{field}: {image.get(field)}")

    region = Region.new(image)

    def extract_and_write_patch(xcoord: int, ycoord: int, image_num: int):
        patch = region.fetch(xcoord, ycoord, patch_size, patch_size)
        patch_image = Image.new_from_memory(patch, patch_size, patch_size, image.bands, image.format)
        out_filename = "{:03d}_{}_{}.jpg".format(image_num, xcoord, ycoord)
        patch_image.write_to_file(out_filename, Q=out_quality)

    for i in range(n_patches):
        x = random.randint(0, image.width - patch_size)
        y = random.randint(0, image.height - patch_size)
        extract_and_write_patch(x, y, i)


if __name__ == "__main__":
    main(sys.argv[1])
