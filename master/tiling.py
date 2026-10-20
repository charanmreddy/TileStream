"""Split an image into tiles (with a small halo) and stitch processed tiles back."""
import cv2
import numpy as np

from common import config, operations


def compute_grid(width, height, tile=None):
    """Return [(x, y, w, h), ...]. Every tile is >= `tile` px in both directions:
    we use floor(W/tile) columns and spread the remainder evenly, so there are
    never tiny leftover tiles on the right/bottom edge."""
    tile = tile or config.TILE_SIZE
    cols = max(1, width // tile)
    rows = max(1, height // tile)
    xs = np.linspace(0, width, cols + 1).round().astype(int)
    ys = np.linspace(0, height, rows + 1).round().astype(int)
    return [(int(xs[c]), int(ys[r]), int(xs[c + 1] - xs[c]), int(ys[r + 1] - ys[r]))
            for r in range(rows) for c in range(cols)]


def make_task(img, x, y, w, h):
    """Cut tile (x,y,w,h) plus halo. Returns (png_bytes, meta_dict)."""
    H, W = img.shape[:2]
    x0, y0 = max(0, x - config.HALO), max(0, y - config.HALO)
    x1, y1 = min(W, x + w + config.HALO), min(H, y + h + config.HALO)
    padded = np.ascontiguousarray(img[y0:y1, x0:x1])
    meta = {"x": x, "y": y, "cw": w, "ch": h, "px": x - x0, "py": y - y0}
    return operations.encode_png(padded), meta


def stitch(width, height, tiles):
    """tiles: iterable of (x, y, w, h, path). Returns the full BGR image."""
    canvas = np.zeros((height, width, 3), np.uint8)
    for x, y, w, h, path in tiles:
        t = cv2.imread(str(path), cv2.IMREAD_COLOR)
        if t is None or t.shape[0] != h or t.shape[1] != w:
            raise RuntimeError(f"tile at ({x},{y}) is missing or has the wrong size")
        canvas[y:y + h, x:x + w] = t
    return canvas
