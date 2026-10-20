"""Image operations. Every filter here uses a kernel radius <= config.HALO,
so tiles processed independently stitch back together without visible seams."""
import cv2
import numpy as np

OPERATIONS = {
    "grayscale": "Grayscale",
    "blur": "Gaussian blur",
    "sharpen": "Sharpen (unsharp mask)",
    "edges": "Edge detection (Sobel)",
    "sepia": "Sepia tone",
    "invert": "Invert colours",
    "denoise": "Bilateral denoise (CPU heavy)",
}


def _to_bgr(gray):
    return cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)


def _grayscale(img):
    return _to_bgr(cv2.cvtColor(img, cv2.COLOR_BGR2GRAY))


def _blur(img):
    return cv2.GaussianBlur(img, (0, 0), sigmaX=3)              # radius ~9 px


def _sharpen(img):
    soft = cv2.GaussianBlur(img, (0, 0), sigmaX=2)              # radius ~6 px
    return cv2.addWeighted(img, 1.6, soft, -0.6, 0)


def _edges(img):
    # Sobel is purely local (radius 1 + 2 for the pre-blur). Canny's hysteresis step can follow an
    # edge across tile borders, so it would NOT give identical output when tiled.
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (5, 5), 0)
    gx = cv2.Sobel(gray, cv2.CV_16S, 1, 0, ksize=3)
    gy = cv2.Sobel(gray, cv2.CV_16S, 0, 1, ksize=3)
    mag = cv2.addWeighted(cv2.convertScaleAbs(gx), 0.5, cv2.convertScaleAbs(gy), 0.5, 0)
    return _to_bgr(cv2.convertScaleAbs(mag, alpha=2.0))


def _sepia(img):
    # rows = output B,G,R ; columns = input B,G,R
    m = np.array([[0.131, 0.534, 0.272],
                  [0.168, 0.686, 0.349],
                  [0.189, 0.769, 0.393]], dtype=np.float32)
    return cv2.transform(img, m)


def _invert(img):
    return cv2.bitwise_not(img)


def _denoise(img):
    out = cv2.bilateralFilter(img, 9, 75, 75)                   # radius 4 px
    return cv2.bilateralFilter(out, 9, 75, 75)                  # total radius 8 px


_FUNCS = {
    "grayscale": _grayscale, "blur": _blur, "sharpen": _sharpen, "edges": _edges,
    "sepia": _sepia, "invert": _invert, "denoise": _denoise,
}


def apply(op, img):
    if op not in _FUNCS:
        raise ValueError(f"unknown operation: {op}")
    return _FUNCS[op](img)


def decode(data):
    """bytes -> BGR uint8 image (or None if not an image)."""
    arr = np.frombuffer(data, np.uint8)
    return cv2.imdecode(arr, cv2.IMREAD_COLOR)


def encode_png(img):
    ok, buf = cv2.imencode(".png", img, [cv2.IMWRITE_PNG_COMPRESSION, 1])
    if not ok:
        raise RuntimeError("PNG encode failed")
    return buf.tobytes()


def process_tile_bytes(op, payload, px, py, cw, ch):
    """What a worker does for one task: decode padded tile -> filter -> crop halo -> PNG."""
    img = decode(payload)
    if img is None:
        raise ValueError("tile payload is not a valid image")
    out = apply(op, img)
    return encode_png(out[py:py + ch, px:px + cw])
