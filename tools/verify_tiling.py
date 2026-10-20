"""Proves tiled processing == whole-image processing (no Kafka needed).
Run:  python -m tools.verify_tiling"""
import pathlib
import tempfile

import numpy as np

from common import operations as ops
from master import tiling
from tools.make_sample import make_image

SIZE = 2048
img = make_image(SIZE)
grid = tiling.compute_grid(SIZE, SIZE)
print(f"Image {SIZE}x{SIZE} -> {len(grid)} tiles of {grid[0][2]}x{grid[0][3]}")
for op in ops.OPERATIONS:
    d = pathlib.Path(tempfile.mkdtemp())
    tiles = []
    for i, (x, y, w, h) in enumerate(grid):
        payload, m = tiling.make_task(img, x, y, w, h)
        out = ops.process_tile_bytes(op, payload, m["px"], m["py"], m["cw"], m["ch"])
        p = d / f"{i}.png"
        p.write_bytes(out)
        tiles.append((x, y, w, h, p))
    stitched = tiling.stitch(SIZE, SIZE, tiles)
    diff = int(np.abs(stitched.astype(int) - ops.apply(op, img).astype(int)).max())
    print(f"  {op:<10} max pixel difference vs single-pass: {diff}  {'OK' if diff == 0 else 'MISMATCH'}")
