"""Generate synthetic test images (no download needed).
Run:  python -m tools.make_sample            -> samples/sample_1024.jpg, 2048, 4096"""
import sys
from pathlib import Path

import cv2
import numpy as np

OUT = Path(__file__).resolve().parent.parent / "samples"


def make_image(size, seed=7):
    """A colourful, detailed picture: gradients + circles + lines + mild noise."""
    rng = np.random.default_rng(seed)
    yy, xx = np.mgrid[0:size, 0:size].astype(np.float32) / size
    img = np.stack([
        127 + 127 * np.sin(6 * xx + 2 * yy),
        127 + 127 * np.sin(5 * yy - 3 * xx + 1),
        127 + 127 * np.cos(4 * xx * yy * 3 + xx),
    ], axis=-1).astype(np.uint8)
    for _ in range(size // 12):
        c = tuple(int(v) for v in rng.integers(0, 255, 3))
        p = (int(rng.integers(0, size)), int(rng.integers(0, size)))
        if rng.random() < 0.5:
            cv2.circle(img, p, int(rng.integers(8, size // 14)), c, -1)
        else:
            q = (int(rng.integers(0, size)), int(rng.integers(0, size)))
            cv2.line(img, p, q, c, int(rng.integers(1, 6)))
    noise = rng.normal(0, 10, img.shape)
    return np.clip(img.astype(np.float32) + noise, 0, 255).astype(np.uint8)


def save_sample(size):
    OUT.mkdir(exist_ok=True)
    path = OUT / f"sample_{size}.jpg"
    cv2.imwrite(str(path), make_image(size), [cv2.IMWRITE_JPEG_QUALITY, 92])
    return path


if __name__ == "__main__":
    sizes = [int(s) for s in sys.argv[1:]] or [1024, 2048, 4096]
    for s in sizes:
        print("wrote", save_sample(s))
