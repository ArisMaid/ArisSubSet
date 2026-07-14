#!/usr/bin/env python3
"""JSON-lines font worker for ass-subset-service.

The Rust service keeps this process alive and sends one request per line:
  {"id":"...", "op":"inspect", "path":"/fonts/a.ttc"}
  {"id":"...", "op":"subset", "payload":{...}}

Responses are single JSON lines with the same id.
"""

from __future__ import annotations

import json
import os
import base64
import io
import re
import struct
import sys
from pathlib import Path
from typing import Any

from fontTools import subset
from fontTools.fontBuilder import FontBuilder
from fontTools.ttLib import TTCollection, TTFont
from fontTools.ttLib.tables.DefaultTable import DefaultTable
from fontTools.ttLib.ttCollection import TTCollection as TTCollectionClass
from fontTools.pens.ttGlyphPen import TTGlyphPen
from fontTools.pens.cu2quPen import Cu2QuPen
from fontTools.varLib.instancer import instantiateVariableFont


NAME_KINDS = {
    1: "family",
    2: "subfamily",
    4: "full",
    5: "version",
    6: "postscript",
    16: "typographic_family",
    17: "typographic_subfamily",
    18: "compatible_full",
    20: "postscript_cid",
    21: "wws_family",
    22: "wws_subfamily",
    25: "variations_postscript_prefix",
}


def reply(req_id: str, ok: bool, result: Any = None, error: str | None = None) -> None:
    print(json.dumps({"id": req_id, "ok": ok, "result": result, "error": error}, ensure_ascii=False), flush=True)


def decode_name(record: Any) -> str:
    try:
        return record.toUnicode().strip()
    except Exception:
        try:
            return str(record).strip()
        except Exception:
            return ""


def best_name(font: TTFont, ids: list[int]) -> str | None:
    names = font["name"].names if "name" in font else []
    for name_id in ids:
        english = []
        fallback = []
        for rec in names:
            if rec.nameID != name_id:
                continue
            value = decode_name(rec)
            if not value:
                continue
            if rec.langID in (0x0409, 0):
                english.append(value)
            else:
                fallback.append(value)
        if english:
            return english[0]
        if fallback:
            return fallback[0]
    return None


def all_interesting_names(font: TTFont) -> list[dict[str, str]]:
    out = []
    seen = set()
    if "name" not in font:
        return out
    for rec in font["name"].names:
        kind = NAME_KINDS.get(rec.nameID)
        if not kind:
            continue
        value = decode_name(rec)
        if not value:
            continue
        key = (kind, value.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append({"name": value, "kind": kind})
    return out


def face_info(font: TTFont, index: int) -> dict[str, Any]:
    weight = 400
    italic = False
    if "OS/2" in font:
        os2 = font["OS/2"]
        weight = int(getattr(os2, "usWeightClass", 400) or 400)
        italic = bool(getattr(os2, "fsSelection", 0) & 0x01)
    if "head" in font:
        italic = italic or bool(getattr(font["head"], "macStyle", 0) & 0x02)
    subfamily = best_name(font, [17, 2]) or ""
    low = subfamily.lower()
    if weight == 400:
        if "thin" in low or "hairline" in low:
            weight = 100
        elif "extra light" in low or "extralight" in low or "ultra light" in low or "ultralight" in low:
            weight = 200
        elif "light" in low:
            weight = 300
        elif "medium" in low:
            weight = 500
        elif "semi bold" in low or "semibold" in low or "demi bold" in low or "demibold" in low:
            weight = 600
        elif "extra bold" in low or "extrabold" in low or "ultra bold" in low or "ultrabold" in low:
            weight = 800
        elif "black" in low or "heavy" in low:
            weight = 900
        elif "bold" in low:
            weight = 700
    italic = italic or ("italic" in low or "oblique" in low)
    return {
        "ttc_index": index,
        "family": best_name(font, [16, 21, 1]),
        "full_name": best_name(font, [4, 18]),
        "postscript_name": best_name(font, [6, 20]),
        "subfamily": subfamily,
        "version": best_name(font, [5]),
        "weight": weight,
        "italic": italic,
        "names": all_interesting_names(font),
    }


def inspect_font(path: str) -> dict[str, Any]:
    faces = []
    try:
        collection = TTCollection(path, lazy=True)
        try:
            for i, font in enumerate(collection.fonts):
                faces.append(face_info(font, i))
        finally:
            close = getattr(collection, "close", None)
            if close:
                close()
        return {"faces": faces}
    except Exception:
        font = TTFont(path, lazy=True)
        try:
            faces.append(face_info(font, -1))
        finally:
            font.close()
        return {"faces": faces}


def load_font(path: str, ttc_index: int, *, lazy: bool = False) -> TTFont:
    if ttc_index is not None and ttc_index >= 0:
        return TTFont(path, fontNumber=ttc_index, lazy=lazy)
    return TTFont(path, lazy=lazy)


def set_name_table(
    font: TTFont,
    target_family: str,
    subfamily: str,
    randomize_map: dict[str, str] | None,
    service_version: str,
) -> None:
    if "name" not in font:
        return
    name_table = font["name"]
    full = target_family if subfamily == "Regular" else f"{target_family} {subfamily}"
    ps = "".join(ch for ch in f"{target_family}-{subfamily.replace(' ', '')}" if 0x20 <= ord(ch) <= 0x7E)
    replacements = {
        1: target_family,
        2: subfamily,
        4: full,
        6: ps or "Subset-Regular",
        16: target_family,
        17: subfamily,
    }
    desc_suffix = "ASS Subsetter (Docker service)"
    if randomize_map:
        desc_suffix = (
            f"FontSubsetMap: {{original: {randomize_map['original']}, "
            f"subset: {randomize_map['subset']}, ass-subset: 2.7, ass-subset-service: {service_version}}}; "
            "ASS Subsetter (Docker service)"
        )
        replacements[10] = desc_suffix
    for rec in name_table.names:
        if rec.nameID not in replacements:
            continue
        try:
            rec.string = replacements[rec.nameID].encode(rec.getEncoding())
        except Exception:
            rec.string = replacements[rec.nameID].encode("utf-16-be")
            rec.platformID = 3
            rec.platEncID = 1
            rec.langID = 0x0409
    existing_ids = {rec.nameID for rec in name_table.names}
    for name_id, value in replacements.items():
        if name_id not in existing_ids:
            name_table.setName(value, name_id, 3, 1, 0x0409)


def present_codepoints(font: TTFont, requested: list[int]) -> list[int]:
    cmap = {}
    for table in font["cmap"].tables if "cmap" in font else []:
        if table.isUnicode():
            cmap.update(table.cmap)
    return [cp for cp in requested if cp in cmap]


def subset_font(payload: dict[str, Any]) -> dict[str, Any]:
    source_path = payload["source_path"]
    output_path = Path(payload["output_path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ttc_index = int(payload.get("ttc_index", -1))
    codepoints = sorted({int(cp) for cp in payload.get("codepoints", [])})
    if payload.get("include_ascii", True):
        codepoints = sorted(set(codepoints).union(range(0x20, 0x7F)))
    full_font = bool(payload.get("full_font", False))
    # Full embedding deliberately keeps untouched tables lazy. This allows a
    # malformed cmap to be copied verbatim while the name table is rewritten.
    font = load_font(source_path, ttc_index, lazy=full_font)
    try:
        orig_size = os.path.getsize(source_path)
        if "fvar" in font and not payload.get("retain_variations", False):
            font = instantiateVariableFont(font, {}, inplace=True, optimize=True)
        used = codepoints if full_font else present_codepoints(font, codepoints)
        if not full_font:
            opts = subset.Options()
            opts.name_IDs = ["*"]
            opts.name_legacy = True
            opts.name_languages = ["*"]
            opts.layout_features = ["*"]
            opts.glyph_names = True
            opts.recalc_bounds = True
            opts.recalc_timestamp = False
            sub = subset.Subsetter(options=opts)
            sub.populate(unicodes=used)
            sub.subset(font)
        set_name_table(
            font,
            payload["target_family"],
            payload.get("subfamily") or "Regular",
            payload.get("randomize_map"),
            str(payload.get("service_version") or "unknown"),
        )
        font.flavor = None
        font.save(output_path)
        subset_size = output_path.stat().st_size
        return {
            "output_path": str(output_path),
            "orig_size": int(orig_size),
            "subset_size": int(subset_size),
            "used_codepoints": used,
        }
    finally:
        font.close()


def parse_font_subset_map(value: str) -> dict[str, str] | None:
    marker = "FontSubsetMap:"
    if marker not in value:
        return None
    start = value.find("{", value.find(marker))
    end = value.find("}", start + 1)
    if start < 0 or end < 0:
        return None
    body = value[start + 1:end]
    parsed: dict[str, str] = {}
    for part in body.split(","):
        if ":" not in part:
            continue
        key, raw = part.split(":", 1)
        parsed[key.strip()] = raw.strip()
    if not parsed.get("original") or not parsed.get("subset"):
        return None
    return {
        "original": parsed["original"],
        "subset": parsed["subset"],
        "version": parsed.get("ass-subset"),
    }


def read_font_subset_map(font: TTFont) -> dict[str, str] | None:
    if "name" not in font:
        return None
    for rec in font["name"].names:
        if rec.nameID != 10:
            continue
        parsed = parse_font_subset_map(decode_name(rec))
        if parsed:
            return parsed
    return None


def serialize_draw_table(entries: list[dict[str, Any]]) -> bytes:
    out = bytearray()
    out.extend(struct.pack(">I", len(entries)))
    for entry in entries:
        data = str(entry["data"]).encode("utf-8")
        ch = str(entry["ch"]).encode("utf-8")
        flags = int(entry.get("flags", 0)) & 0xFF
        if len(data) > 0xFFFF:
            raise ValueError("draw entry data is too long")
        if len(ch) > 0xFF:
            raise ValueError("draw entry char is too long")
        out.extend(struct.pack(">H", len(data)))
        out.extend(data)
        out.append(len(ch))
        out.extend(ch)
        out.append(flags)
    return bytes(out)


def parse_draw_table(raw: bytes) -> list[dict[str, Any]]:
    if len(raw) < 4:
        return []
    count = struct.unpack(">I", raw[:4])[0]
    pos = 4
    entries = []
    for _ in range(count):
        if pos + 2 > len(raw):
            break
        data_len = struct.unpack(">H", raw[pos:pos + 2])[0]
        pos += 2
        if pos + data_len + 2 > len(raw):
            break
        data = raw[pos:pos + data_len].decode("utf-8", errors="replace")
        pos += data_len
        ch_len = raw[pos]
        pos += 1
        if pos + ch_len + 1 > len(raw):
            break
        ch = raw[pos:pos + ch_len].decode("utf-8", errors="replace")
        pos += ch_len
        flags = raw[pos]
        pos += 1
        entries.append({"data": data, "ch": ch, "flags": flags})
    return entries


def inspect_embedded_font(payload: dict[str, Any]) -> dict[str, Any]:
    raw = base64.b64decode(payload["font_b64"])
    font = TTFont(io.BytesIO(raw), lazy=False)
    try:
        draw_entries = []
        if "draw" in font:
            table = font["draw"]
            data = getattr(table, "data", b"")
            draw_entries = parse_draw_table(data)
        return {
            "font_subset_map": read_font_subset_map(font),
            "draw_entries": draw_entries,
        }
    finally:
        font.close()


DRAW_TOKEN_RE = re.compile(r"[mnlbspcMNLBSPC]|[-+]?(?:\d*\.\d+|\d+)")


def parse_draw_ops(data: str) -> list[tuple[str, list[float]]]:
    tokens = DRAW_TOKEN_RE.findall(data)
    ops: list[tuple[str, list[float]]] = []
    cmd = ""
    i = 0
    arity = {"m": 2, "n": 2, "l": 2, "b": 6, "s": 2, "p": 2, "c": 0}
    while i < len(tokens):
        token = tokens[i]
        if token.isalpha():
            cmd = token.lower()
            i += 1
            if cmd == "c":
                ops.append((cmd, []))
            continue
        if not cmd:
            i += 1
            continue
        need = arity.get(cmd, 2)
        if need == 0:
            continue
        nums: list[float] = []
        while i < len(tokens) and len(nums) < need and not tokens[i].isalpha():
            try:
                nums.append(float(tokens[i]))
            except ValueError:
                pass
            i += 1
        if len(nums) == need:
            ops.append((cmd, nums))
        else:
            break
    return ops


def draw_glyph(data: str, flags: int) -> Any:
    data = re.sub(r"\{[^}]*\}", " ", data)
    ops = parse_draw_ops(data)
    coords = []
    for _, nums in ops:
        coords.extend((nums[i], nums[i + 1]) for i in range(0, len(nums), 2))
    glyph_pen = TTGlyphPen(None)
    pen = Cu2QuPen(glyph_pen, max_err=1.0, reverse_direction=False)
    if not coords:
        return glyph_pen.glyph()
    min_x = min(x for x, _ in coords)
    max_x = max(x for x, _ in coords)
    min_y = min(y for _, y in coords)
    max_y = max(y for _, y in coords)
    width = max(max_x - min_x, 1.0)
    height = max(max_y - min_y, 1.0)
    scale = min(900.0 / width, 900.0 / height)
    p_level = flags & 0x0F
    if p_level > 1:
        scale /= 2 ** (p_level - 1)

    def tx(x: float, y: float) -> tuple[int, int]:
        return (int(round((x - min_x) * scale + 50)), int(round((max_y - y) * scale + 50)))

    has_open = False
    for cmd, nums in ops:
        if cmd in ("m", "n"):
            if has_open:
                pen.closePath()
            pen.moveTo(tx(nums[0], nums[1]))
            has_open = True
        elif cmd in ("l", "s", "p"):
            if not has_open:
                pen.moveTo(tx(nums[0], nums[1]))
                has_open = True
            else:
                pen.lineTo(tx(nums[0], nums[1]))
        elif cmd == "b":
            if not has_open:
                pen.moveTo(tx(nums[0], nums[1]))
                has_open = True
            pen.curveTo(tx(nums[0], nums[1]), tx(nums[2], nums[3]), tx(nums[4], nums[5]))
        elif cmd == "c" and has_open:
            pen.closePath()
            has_open = False
    if has_open:
        pen.closePath()
    return glyph_pen.glyph()


def create_draw_font(payload: dict[str, Any]) -> dict[str, Any]:
    output_path = Path(payload["output_path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    family = payload.get("family") or "ASSDrawSubset"
    entries = list(payload.get("drawings", []))
    glyph_order = [".notdef"] + [f"draw{i}" for i in range(len(entries))]
    fb = FontBuilder(1000, isTTF=True)
    fb.setupGlyphOrder(glyph_order)
    cmap = {}
    glyphs = {".notdef": TTGlyphPen(None).glyph()}
    metrics = {".notdef": (1000, 0)}
    for i, entry in enumerate(entries):
        glyph_name = f"draw{i}"
        ch = str(entry["ch"])
        if ch:
            cmap[ord(ch[0])] = glyph_name
        glyphs[glyph_name] = draw_glyph(str(entry["data"]), int(entry.get("flags", 0)))
        metrics[glyph_name] = (1000, 0)
    fb.setupCharacterMap(cmap)
    fb.setupGlyf(glyphs)
    fb.setupHorizontalMetrics(metrics)
    fb.setupHorizontalHeader(ascent=900, descent=-100)
    fb.setupOS2(sTypoAscender=900, sTypoDescender=-100, usWinAscent=1000, usWinDescent=200)
    fb.setupNameTable({
        "familyName": family,
        "styleName": "Regular",
        "uniqueFontIdentifier": f"{family};ASSDrawSubset",
        "fullName": family,
        "psName": f"{family}-Regular",
        "version": "Version 1.0",
        "description": (
            "ASS draw subset font; ass-subset: 2.7; "
            f"ass-subset-service: {payload.get('service_version') or 'unknown'}"
        ),
    })
    fb.setupPost()
    font = fb.font
    table = DefaultTable("draw")
    table.data = serialize_draw_table(entries)
    font["draw"] = table
    font.save(output_path)
    font.close()
    return {
        "output_path": str(output_path),
        "subset_size": int(output_path.stat().st_size),
        "entries": entries,
    }


def handle(req: dict[str, Any]) -> Any:
    op = req.get("op")
    if op == "inspect":
        return inspect_font(req["path"])
    if op == "subset":
        return subset_font(req["payload"])
    if op == "inspect_embedded":
        return inspect_embedded_font(req["payload"])
    if op == "create_draw_font":
        return create_draw_font(req["payload"])
    raise ValueError(f"unknown op: {op}")


def main() -> None:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        req_id = "unknown"
        try:
            req = json.loads(line)
            req_id = req.get("id", "unknown")
            result = handle(req)
            reply(req_id, True, result=result)
        except Exception as exc:
            reply(req_id, False, error=f"{type(exc).__name__}: {exc}")


if __name__ == "__main__":
    main()
