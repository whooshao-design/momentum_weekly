from __future__ import annotations

import math
import struct
import zlib
from pathlib import Path


def _png_chunk(tag: bytes, data: bytes) -> bytes:
    length = struct.pack(">I", len(data))
    crc = zlib.crc32(tag)
    crc = zlib.crc32(data, crc)
    return length + tag + data + struct.pack(">I", crc & 0xFFFFFFFF)


def _line_points(values: list[float], width: int, height: int, padding: int) -> list[tuple[int, int]]:
    if not values:
        return []

    x_min = padding
    x_max = width - padding - 1
    y_min = padding
    y_max = height - padding - 1

    v_min = min(values)
    v_max = max(values)
    if abs(v_max - v_min) < 1e-12:
        v_max = v_min + 1e-6

    n = len(values)
    points: list[tuple[int, int]] = []
    for idx, value in enumerate(values):
        if n == 1:
            x = (x_min + x_max) // 2
        else:
            x = x_min + int((x_max - x_min) * idx / (n - 1))
        ratio = (value - v_min) / (v_max - v_min)
        y = y_max - int((y_max - y_min) * ratio)
        points.append((x, y))
    return points


def _draw_line(pixels: list[list[tuple[int, int, int]]], p0: tuple[int, int], p1: tuple[int, int], color: tuple[int, int, int]) -> None:
    x0, y0 = p0
    x1, y1 = p1

    dx = abs(x1 - x0)
    dy = abs(y1 - y0)
    sx = 1 if x0 < x1 else -1
    sy = 1 if y0 < y1 else -1
    err = dx - dy

    width = len(pixels[0])
    height = len(pixels)

    while True:
        if 0 <= x0 < width and 0 <= y0 < height:
            pixels[y0][x0] = color
        if x0 == x1 and y0 == y1:
            break
        e2 = 2 * err
        if e2 > -dy:
            err -= dy
            x0 += sx
        if e2 < dx:
            err += dx
            y0 += sy


def _draw_axes(pixels: list[list[tuple[int, int, int]]], width: int, height: int, padding: int) -> None:
    axis_color = (180, 180, 180)
    for x in range(padding, width - padding):
        pixels[height - padding][x] = axis_color
    for y in range(padding, height - padding + 1):
        pixels[y][padding] = axis_color


def save_nav_curve_png(path: str | Path, nav_values: list[float]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)

    width = 960
    height = 480
    padding = 40

    white = (250, 250, 250)
    line_color = (30, 110, 220)
    grid_color = (230, 230, 230)

    pixels: list[list[tuple[int, int, int]]] = [
        [white for _ in range(width)] for _ in range(height)
    ]

    for gy in range(padding, height - padding, 50):
        for x in range(padding, width - padding):
            pixels[gy][x] = grid_color

    _draw_axes(pixels, width, height, padding)

    points = _line_points(nav_values, width, height, padding)
    for idx in range(1, len(points)):
        _draw_line(pixels, points[idx - 1], points[idx], line_color)

    raw = bytearray()
    for row in pixels:
        raw.append(0)
        for (r, g, b) in row:
            raw.extend([r, g, b])

    compressed = zlib.compress(bytes(raw), level=9)
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)

    png = bytearray()
    png.extend(b"\x89PNG\r\n\x1a\n")
    png.extend(_png_chunk(b"IHDR", ihdr))
    png.extend(_png_chunk(b"IDAT", compressed))
    png.extend(_png_chunk(b"IEND", b""))
    output.write_bytes(bytes(png))

