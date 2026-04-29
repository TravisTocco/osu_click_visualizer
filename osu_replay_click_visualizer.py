"""
osu! Replay Visualizer & Miss Analyzer

Render osu! and osu!(lazer) replay files into annotated videos that show cursor movement,
hits, misses, judgments, sliders, timing data, and optional miss analysis outputs.

Features:
- Supports osu! stable and osu!(lazer) replay/export workflows.
- Customizable visual layers, judgment popups, preview panel, FPS, resolution, and quality.
- Fast parallel rendering with progress updates, ETA logging, and organized output folders.
- Optional miss snapshot sheet plus CSV/HTML replay data sheets.

Requirements:
pip install osrparse opencv-python numpy imageio-ffmpeg
"""

import base64
import bisect
import csv
import ctypes
import hashlib
import html
import json
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import cv2
import numpy as np
from osrparse import Replay

try:
    import imageio_ffmpeg
except ImportError:
    imageio_ffmpeg = None

try:
    # Parallel chunk rendering already uses multiple Python processes.
    # Letting every chunk process use every OpenCV thread causes heavy CPU oversubscription.
    if "--render-chunk" in sys.argv:
        cv2.setNumThreads(1)
    else:
        cv2.setNumThreads(max(1, min(4, os.cpu_count() or 1)))
except Exception:
    pass

try:
    # Keep progress visible immediately in the Tkinter UI and chunk log files.
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


# ============================================================
# USER SETTINGS
# ============================================================

SCRIPT_VERSION = "osu_replay_click_visualizer_v32_preview_right_smooth"

# ------------------------------------------------------------
# Config / portability
# ------------------------------------------------------------
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "osu_visualizer_config.json")

DEFAULT_CONFIG = {
    "visual_style": "ghost",                 # "solid" or "ghost"
    "render_encoder": "h264_nvenc",          # "h264_nvenc" or "libx264"
    "quality_profile": "high",             # "fast", "balanced", "high", "max"
    "performance_mode": "quality",        # "quality", "fast", "turbo", or "custom"
    "custom_draw_background": True,
    "custom_draw_playfield_border": True,
    "custom_draw_approach_circles": True,
    "custom_draw_object_numbers": True,
    "custom_draw_cursor_trail": True,
    "custom_draw_click_pulses": True,
    "custom_draw_timeline": True,
    "custom_draw_key_boxes": True,
    "custom_draw_header": True,
    "custom_draw_slider_ticks": True,
    "custom_draw_slider_follow_circle": True,
    "custom_draw_judgments": True,
    "custom_draw_judgment_totals": True,
    "judgment_show_great": False,
    "judgment_text_great": "Great",
    "judgment_text_ok": "100",
    "judgment_text_meh": "50",
    "judgment_text_miss": "Miss",
    "judgment_text_duration_ms": 300,
    "judgment_text_position": "center",    # "center", "above", "below", "left", "right"
    "judgment_text_offset_x": 0,
    "judgment_text_offset_y": 0,
    "judgment_draw_miss_x": False,
    "judgment_show_slider_details": False,
    "parallel_workers": 0,                 # 0 = auto, 1 = off, 2-8 = faster renders
    "render_log_interval_seconds": 10,
    "render_fps": 0,                        # 0 = auto-detect monitor Hz; UI displays detected value
    "render_width": 0,                      # 0 = auto-detect monitor width; UI displays detected value
    "render_height": 0,                     # 0 = auto-detect monitor height; UI displays detected value
    "watch_exports_on_start": False,
    "guide_missing_osz_export": True,
    "output_dir": "osu_visualizer_output",
    "snake_in_duration_ms": 450,
    "generate_miss_sheet": True,
    "save_individual_miss_frames": False,
    "generate_data_sheet": True,
    "data_nearest_click_window_ms": 650,
    "enable_start_ui": True,
    "auto_select_newest_replay": True,
    "osu_install_type": "lazer",             # lazer = osu!(lazer), stable = classic osu!
    "miss_sheet_scale": 2,                    # 2 = sharper high-resolution miss snapshot sheet
    "osu_root_dir": "",                      # blank = auto-detect
    "exports_dir": "",                       # blank = auto-detect; lazer exports or stable Replays
    "replay_path": "",                       # optional exact .osr
    "beatmap_path": "",                      # optional exact .osu
}


def cli_arg_value(flag: str, default: Optional[str] = None) -> Optional[str]:
    try:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    except ValueError:
        pass
    return default


def load_config() -> Dict:
    if not os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, indent=2)
        print("Created config file:", CONFIG_PATH)
        return dict(DEFAULT_CONFIG)

    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            user_cfg = json.load(f)
    except Exception as exc:
        print("Could not read config file, using defaults:", exc)
        return dict(DEFAULT_CONFIG)

    cfg = dict(DEFAULT_CONFIG)
    if isinstance(user_cfg, dict):
        cfg.update(user_cfg)

    # v32 progress patch: older generated config files often contain the old
    # 5-second interval, which would override the new 10-second default forever.
    if cfg.get("render_log_interval_seconds") == 5:
        cfg["render_log_interval_seconds"] = 10
        try:
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2)
            print("Updated render_log_interval_seconds to 10 seconds in config.")
        except Exception as exc:
            print("Could not update render_log_interval_seconds in config:", exc)

    return cfg


def normalize_osu_install_type(value: str) -> str:
    text = str(value or "lazer").strip().lower()
    if "stable" in text or text in ("osu", "osu!", "classic"):
        return "stable"
    return "lazer"


def replay_folder_name_for_install_type(osu_install_type: str) -> str:
    return "Replays" if normalize_osu_install_type(osu_install_type) == "stable" else "exports"


def auto_detect_osu_paths(osu_install_type: str = "lazer") -> Tuple[str, str]:
    home = Path.home()
    appdata = os.environ.get("APPDATA")
    local = os.environ.get("LOCALAPPDATA")
    kind = normalize_osu_install_type(osu_install_type)

    lazer_candidates: List[Path] = []
    stable_candidates: List[Path] = []
    if appdata:
        lazer_candidates.append(Path(appdata) / "osu")       # osu!lazer default
        stable_candidates.append(Path(appdata) / "osu!")
    if local:
        stable_candidates.append(Path(local) / "osu!")       # osu!stable default
        lazer_candidates.append(Path(local) / "osu")
    lazer_candidates.extend([
        home / "AppData" / "Roaming" / "osu",
        home / "AppData" / "Local" / "osu",
    ])
    stable_candidates.extend([
        home / "AppData" / "Local" / "osu!",
        home / "AppData" / "Roaming" / "osu!",
    ])

    candidates = stable_candidates + lazer_candidates if kind == "stable" else lazer_candidates + stable_candidates
    preferred_folder = replay_folder_name_for_install_type(kind)

    seen = set()
    unique_candidates: List[Path] = []
    for root in candidates:
        key = str(root).lower()
        if key in seen:
            continue
        seen.add(key)
        unique_candidates.append(root)

    for root in unique_candidates:
        if root.exists():
            replay_folder = root / preferred_folder
            if replay_folder.exists():
                return str(root), str(replay_folder)

    for root in unique_candidates:
        if root.exists():
            return str(root), str(root / preferred_folder)

    return "", ""


CONFIG = load_config()
OSU_INSTALL_TYPE = normalize_osu_install_type(CONFIG.get("osu_install_type", "lazer"))
AUTO_OSU_ROOT, AUTO_EXPORTS_DIR = auto_detect_osu_paths(OSU_INSTALL_TYPE)

# osu! folder / exports folder. Blank config values use auto-detection.
OSU_FOLDER = CONFIG.get("osu_root_dir") or AUTO_OSU_ROOT
REPLAY_FOLDER = CONFIG.get("exports_dir") or AUTO_EXPORTS_DIR
OSZ_SEARCH_FOLDERS = [REPLAY_FOLDER] if REPLAY_FOLDER else []

# Optional exact paths. Leave blank for automatic newest replay + automatic beatmap matching.
REPLAY_PATH = cli_arg_value("--replay-path", CONFIG.get("replay_path", "")) or ""
BEATMAP_PATH = cli_arg_value("--beatmap-path", CONFIG.get("beatmap_path", "")) or ""

BASE_OUTPUT_DIR = str(CONFIG.get("output_dir", "osu_visualizer_output") or "osu_visualizer_output")
OUTPUT_DIR = BASE_OUTPUT_DIR
OUTPUT_VIDEO_PATH = os.path.join(OUTPUT_DIR, "osu_replay_visualizer_v32.mp4")
TEMP_VIDEO_PATH = os.path.join(OUTPUT_DIR, "osu_replay_visualizer_v32_silent.mp4")
EXTRACTED_OSZ_DIR = os.path.join(OUTPUT_DIR, "extracted_osz")

# Miss snapshot sheet output.
MISS_FRAME_DIR = os.path.join(OUTPUT_DIR, "miss_frames_v32")
MISS_SHEET_PATH = os.path.join(OUTPUT_DIR, "osu_replay_miss_sheet_v32.jpg")
GENERATE_MISS_SHEET = bool(CONFIG.get("generate_miss_sheet", True))
SAVE_INDIVIDUAL_MISS_FRAMES = bool(CONFIG.get("save_individual_miss_frames", False))
MISS_SHEET_COLUMNS = 3
MISS_SHEET_THUMB_WIDTH = 640
MISS_SHEET_PADDING = 20
MISS_SHEET_CAPTION_HEIGHT = 56
MISS_SHEET_SCALE = max(1, min(3, int(CONFIG.get("miss_sheet_scale", 2) or 2)))
MISS_SHEET_JPEG_QUALITY = 98

# Data report output.
DATA_CSV_PATH = os.path.join(OUTPUT_DIR, "osu_replay_data_sheet_v32.csv")
DATA_HTML_PATH = os.path.join(OUTPUT_DIR, "osu_replay_data_sheet_v32.html")
GENERATE_DATA_SHEET = bool(CONFIG.get("generate_data_sheet", True))
DATA_NEAREST_CLICK_WINDOW_MS = int(CONFIG.get("data_nearest_click_window_ms", 650))
ENABLE_START_UI = bool(CONFIG.get("enable_start_ui", True))

# Native/high-Hz output.
AUTO_NATIVE_DISPLAY = True
FALLBACK_OUTPUT_WIDTH = 2560
FALLBACK_OUTPUT_HEIGHT = 1440
FALLBACK_OUTPUT_FPS = 240
OUTPUT_WIDTH = FALLBACK_OUTPUT_WIDTH
OUTPUT_HEIGHT = FALLBACK_OUTPUT_HEIGHT
OUTPUT_FPS = FALLBACK_OUTPUT_FPS

# User-friendly quality/FPS/resolution controls from config/UI.
CONFIG_RENDER_FPS = int(CONFIG.get("render_fps", 0) or 0)
CONFIG_RENDER_WIDTH = int(CONFIG.get("render_width", 0) or 0)
CONFIG_RENDER_HEIGHT = int(CONFIG.get("render_height", 0) or 0)
QUALITY_PROFILE = str(CONFIG.get("quality_profile", "high")).strip().lower()
if QUALITY_PROFILE not in ("fast", "balanced", "high", "max"):
    QUALITY_PROFILE = "high"
PERFORMANCE_MODE = str(CONFIG.get("performance_mode", "quality")).strip().lower()
if PERFORMANCE_MODE not in ("quality", "fast", "turbo", "custom"):
    PERFORMANCE_MODE = "quality"
PARALLEL_WORKERS_CONFIG = int(CONFIG.get("parallel_workers", 0) or 0)
SMART_CHUNK_TARGET_SECONDS = 8
RENDER_LOG_INTERVAL_SECONDS = float(CONFIG.get("render_log_interval_seconds", 10) or 10)
# Watch mode is only used when launched with --watch, so Start Render Now always uses the newest existing replay.
WATCH_EXPORTS_ON_START = False
GUIDE_MISSING_OSZ_EXPORT = bool(CONFIG.get("guide_missing_osz_export", True))

# GPU video encoding.
RENDER_ENCODER = CONFIG.get("render_encoder", "h264_nvenc")  # "h264_nvenc" or "libx264"
NVENC_RENDER_PRESET = "p1"     # p1 fastest, p7 slowest/better compression
NVENC_RENDER_QP = 18           # lower = better quality/larger file
X264_RENDER_PRESET = "veryfast"
X264_RENDER_CRF = 18


def apply_quality_profile() -> None:
    global NVENC_RENDER_PRESET, NVENC_RENDER_QP, X264_RENDER_PRESET, X264_RENDER_CRF
    # Friendly presets exposed in the UI. Lower QP/CRF = better quality, larger file.
    if QUALITY_PROFILE == "fast":
        NVENC_RENDER_PRESET = "p1"
        NVENC_RENDER_QP = 24
        X264_RENDER_PRESET = "ultrafast"
        X264_RENDER_CRF = 25
    elif QUALITY_PROFILE == "balanced":
        NVENC_RENDER_PRESET = "p1"
        NVENC_RENDER_QP = 21
        X264_RENDER_PRESET = "veryfast"
        X264_RENDER_CRF = 22
    elif QUALITY_PROFILE == "max":
        NVENC_RENDER_PRESET = "p4"
        NVENC_RENDER_QP = 14
        X264_RENDER_PRESET = "slow"
        X264_RENDER_CRF = 14
    else:  # high
        NVENC_RENDER_PRESET = "p1"
        NVENC_RENDER_QP = 18
        X264_RENDER_PRESET = "veryfast"
        X264_RENDER_CRF = 18


def resolve_parallel_workers() -> int:
    if "--render-chunk" in sys.argv:
        return 1
    if PARALLEL_WORKERS_CONFIG > 0:
        # More workers can speed up CPU-heavy renders, but too many may overload Windows/NVENC/disk IO.
        return max(1, min(8, PARALLEL_WORKERS_CONFIG))
    # Auto: conservative default. This keeps the PC usable and avoids NVENC/session/disk overload.
    cpu = os.cpu_count() or 2
    return max(1, min(3, cpu // 2))
    global NVENC_RENDER_PRESET, NVENC_RENDER_QP, X264_RENDER_PRESET, X264_RENDER_CRF
    # Friendly presets exposed in the UI. Lower QP/CRF = better quality, larger file.
    if QUALITY_PROFILE == "fast":
        NVENC_RENDER_PRESET = "p1"
        NVENC_RENDER_QP = 24
        X264_RENDER_PRESET = "ultrafast"
        X264_RENDER_CRF = 25
    elif QUALITY_PROFILE == "balanced":
        NVENC_RENDER_PRESET = "p1"
        NVENC_RENDER_QP = 21
        X264_RENDER_PRESET = "veryfast"
        X264_RENDER_CRF = 22
    elif QUALITY_PROFILE == "max":
        NVENC_RENDER_PRESET = "p4"
        NVENC_RENDER_QP = 14
        X264_RENDER_PRESET = "slow"
        X264_RENDER_CRF = 14
    else:  # high
        NVENC_RENDER_PRESET = "p1"
        NVENC_RENDER_QP = 18
        X264_RENDER_PRESET = "veryfast"
        X264_RENDER_CRF = 18


apply_quality_profile()

# Replay selection.
MAX_REPLAY_CANDIDATES_TO_PARSE = 250
PRINT_REPLAY_CANDIDATES = True

# Render timeline.
RENDER_START_MODE = "song_start"  # "song_start" or "auto_first_object"
RENDER_END_MODE = "replay_end"    # "replay_end" or "map_end"
RENDER_END_PADDING_MS = 800

# Timing calibration.
AUTO_CALIBRATE_TIMING = True
USE_AUTO_OFFSET_PLUS_MANUAL = True
TIMING_CALIBRATION_SEARCH_MS = 3500
TIMING_CALIBRATION_STEP_MS = 5
TIMING_CALIBRATION_MAX_CLICKS = 220
TIMING_CALIBRATION_MAX_OBJECTS = 260
TIMING_HIT_WINDOW_MS = 150

# Main user timing knob.
# Positive = input/click visuals happen later in the video.
# Negative = input/click visuals happen earlier.
MANUAL_REPLAY_TO_SONG_OFFSET_MS = 0

# If lazer replay records presses as M1/M2 rather than K1/K2, use them as fallback.
USE_MOUSE_BUTTON_FALLBACK_FOR_CLICKS = True

# osu!-style screen-space mapping.
# Standard osu! playfield coordinates are 512x384 inside a 640x480 base area.
# At 2560x1440, this gives scale=3, playfield=1536x1152, origin=(512,144).
USE_OSU_SCREENSPACE_MAPPING = True

# Visual settings.
DRAW_BACKGROUND = True
DARKEN_BACKGROUND = 0.78
DRAW_PLAYFIELD_BORDER = True
DRAW_PLAYFIELD_GRID = False
DRAW_BEATMAP_OBJECTS = True
DRAW_APPROACH_CIRCLES = True
DRAW_OBJECT_NUMBERS = True
DRAW_CURSOR_TRAIL = True
DRAW_CLICK_PULSES = True
DRAW_TIMELINE = True
DRAW_KEY_BOXES = True
DRAW_HEADER = True
DRAW_SLIDER_TICKS = True
DRAW_JUDGMENTS = True
DRAW_JUDGMENT_TOTALS = True

# v28 visual style.
# "solid" = current opaque look. "ghost" = see-through osu-like look.
VISUAL_STYLE = str(CONFIG.get("visual_style", "ghost")).strip().lower()
if VISUAL_STYLE not in ("solid", "ghost"):
    VISUAL_STYLE = "ghost"
GHOST_CIRCLE_ALPHA = 0.45
GHOST_SLIDER_ALPHA = 0.30
GHOST_SLIDER_BALL_ALPHA = 0.55
GHOST_FOLLOW_CIRCLE_ALPHA = 0.22

# v23+ slider visual settings.
DRAW_SLIDER_BALL = True
DRAW_SLIDER_FOLLOW_CIRCLE = True
ENABLE_SNAKING_IN = True
ENABLE_SNAKING_OUT = True
COLOR_OBJECTS_BY_RESULT = True

# osu!'s slider snake-in is faster than the full approach-circle duration.
# Lower = snappier/faster snake-in. 450ms is a good starting point for osu!-like feel.
SNAKE_IN_DURATION_MS = int(CONFIG.get("snake_in_duration_ms", 450))
FOLLOW_CIRCLE_RADIUS_SCALE = 2.2
SLIDER_BALL_RADIUS_SCALE = 0.90

LABEL_K1 = "A"
LABEL_K2 = "D"
PULSE_DURATION_MS = 360
FLASH_DURATION_MS = 90
TRAIL_MS = 160
TIMELINE_WINDOW_MS = 3000
JUDGMENT_TEXT_MS = max(50, int(CONFIG.get("judgment_text_duration_ms", 300) or 300))
JUDGMENT_SHOW_GREAT = bool(CONFIG.get("judgment_show_great", False))
JUDGMENT_TEXT_LABELS = {
    "Great": str(CONFIG.get("judgment_text_great", "Great")),
    "Ok": str(CONFIG.get("judgment_text_ok", "100")),
    "Meh": str(CONFIG.get("judgment_text_meh", "50")),
    "Miss": str(CONFIG.get("judgment_text_miss", "Miss")),
}
JUDGMENT_TEXT_POSITION = str(CONFIG.get("judgment_text_position", "center")).strip().lower()
if JUDGMENT_TEXT_POSITION not in ("center", "above", "below", "left", "right"):
    JUDGMENT_TEXT_POSITION = "center"
JUDGMENT_TEXT_OFFSET_X = int(CONFIG.get("judgment_text_offset_x", 0) or 0)
JUDGMENT_TEXT_OFFSET_Y = int(CONFIG.get("judgment_text_offset_y", 0) or 0)
JUDGMENT_DRAW_MISS_X = bool(CONFIG.get("judgment_draw_miss_x", False))
JUDGMENT_SHOW_SLIDER_DETAILS = bool(CONFIG.get("judgment_show_slider_details", False))
MISS_X_SIZE = 22

# Slider/debug judgment leniency.
# osu! has internal hit object radius/scoring details. These values make the debug overlay useful
# without being so strict that tiny rendering differences make everything look wrong.
CIRCLE_HIT_RADIUS_MULTIPLIER = 1.08
SLIDER_BODY_HIT_RADIUS_MULTIPLIER = 1.35

# BGR colors.
COLOR_BG = (12, 12, 16)
COLOR_FIELD = (18, 18, 24)
COLOR_GRID = (45, 45, 55)
COLOR_TEXT = (245, 245, 245)
COLOR_SHADOW = (0, 0, 0)
COLOR_CURSOR = (255, 255, 255)
COLOR_K1 = (255, 120, 0)
COLOR_K2 = (0, 190, 255)
COLOR_OBJECT = (235, 235, 245)
COLOR_APPROACH = (170, 170, 190)
COLOR_HIT_PAST = (70, 70, 78)
COLOR_PANEL = (20, 20, 26)
COLOR_HELD = (70, 210, 90)
COLOR_UP = (65, 65, 65)
COMBO_COLORS = [(255, 140, 80), (80, 200, 255), (140, 230, 120), (220, 130, 255)]

COLOR_GREAT = (90, 220, 90)
COLOR_OK = (70, 190, 255)
COLOR_MEH = (0, 220, 255)
COLOR_MISS = (60, 60, 255)
COLOR_SLIDER_TICK = (240, 240, 240)


def apply_performance_mode() -> None:
    """Apply optional render-detail reductions for faster output.

    quality = original visual detail.
    fast    = keeps gameplay/click readability, removes some expensive debug cosmetics.
    turbo   = fastest preview-style output, minimal overlays/background work.
    custom  = user-selected visual layers from the UI/config.
    """
    global DRAW_BACKGROUND, DRAW_APPROACH_CIRCLES, DRAW_OBJECT_NUMBERS, DRAW_CURSOR_TRAIL
    global DRAW_CLICK_PULSES, DRAW_TIMELINE, DRAW_KEY_BOXES, DRAW_HEADER
    global DRAW_PLAYFIELD_BORDER, DRAW_SLIDER_TICKS, DRAW_SLIDER_FOLLOW_CIRCLE, DRAW_JUDGMENTS, DRAW_JUDGMENT_TOTALS

    if PERFORMANCE_MODE == "fast":
        DRAW_CURSOR_TRAIL = False
        DRAW_OBJECT_NUMBERS = False
        DRAW_SLIDER_TICKS = False
        DRAW_TIMELINE = False
        # Fast mode keeps the judgment totals HUD enabled for readability.
    elif PERFORMANCE_MODE == "turbo":
        DRAW_BACKGROUND = False
        DRAW_PLAYFIELD_BORDER = False
        DRAW_APPROACH_CIRCLES = False
        DRAW_OBJECT_NUMBERS = False
        DRAW_CURSOR_TRAIL = False
        DRAW_TIMELINE = False
        DRAW_HEADER = False
        DRAW_SLIDER_TICKS = False
        DRAW_SLIDER_FOLLOW_CIRCLE = False
        DRAW_JUDGMENT_TOTALS = False  # Turbo intentionally removes most overlays for speed.
    elif PERFORMANCE_MODE == "custom":
        DRAW_BACKGROUND = bool(CONFIG.get("custom_draw_background", DRAW_BACKGROUND))
        DRAW_PLAYFIELD_BORDER = bool(CONFIG.get("custom_draw_playfield_border", DRAW_PLAYFIELD_BORDER))
        DRAW_APPROACH_CIRCLES = bool(CONFIG.get("custom_draw_approach_circles", DRAW_APPROACH_CIRCLES))
        DRAW_OBJECT_NUMBERS = bool(CONFIG.get("custom_draw_object_numbers", DRAW_OBJECT_NUMBERS))
        DRAW_CURSOR_TRAIL = bool(CONFIG.get("custom_draw_cursor_trail", DRAW_CURSOR_TRAIL))
        DRAW_CLICK_PULSES = bool(CONFIG.get("custom_draw_click_pulses", DRAW_CLICK_PULSES))
        DRAW_TIMELINE = bool(CONFIG.get("custom_draw_timeline", DRAW_TIMELINE))
        DRAW_KEY_BOXES = bool(CONFIG.get("custom_draw_key_boxes", DRAW_KEY_BOXES))
        DRAW_HEADER = bool(CONFIG.get("custom_draw_header", DRAW_HEADER))
        DRAW_SLIDER_TICKS = bool(CONFIG.get("custom_draw_slider_ticks", DRAW_SLIDER_TICKS))
        DRAW_SLIDER_FOLLOW_CIRCLE = bool(CONFIG.get("custom_draw_slider_follow_circle", DRAW_SLIDER_FOLLOW_CIRCLE))
        DRAW_JUDGMENTS = bool(CONFIG.get("custom_draw_judgments", DRAW_JUDGMENTS))
        DRAW_JUDGMENT_TOTALS = bool(CONFIG.get("custom_draw_judgment_totals", DRAW_JUDGMENT_TOTALS))


apply_performance_mode()


# ============================================================
# DISPLAY DETECTION
# ============================================================


def apply_native_display_settings() -> None:
    global OUTPUT_WIDTH, OUTPUT_HEIGHT, OUTPUT_FPS
    if not AUTO_NATIVE_DISPLAY or os.name != "nt":
        return

    try:
        class DEVMODEW(ctypes.Structure):
            _fields_ = [
                ("dmDeviceName", ctypes.c_wchar * 32),
                ("dmSpecVersion", ctypes.c_ushort),
                ("dmDriverVersion", ctypes.c_ushort),
                ("dmSize", ctypes.c_ushort),
                ("dmDriverExtra", ctypes.c_ushort),
                ("dmFields", ctypes.c_ulong),
                ("dmOrientation", ctypes.c_short),
                ("dmPaperSize", ctypes.c_short),
                ("dmPaperLength", ctypes.c_short),
                ("dmPaperWidth", ctypes.c_short),
                ("dmScale", ctypes.c_short),
                ("dmCopies", ctypes.c_short),
                ("dmDefaultSource", ctypes.c_short),
                ("dmPrintQuality", ctypes.c_short),
                ("dmColor", ctypes.c_short),
                ("dmDuplex", ctypes.c_short),
                ("dmYResolution", ctypes.c_short),
                ("dmTTOption", ctypes.c_short),
                ("dmCollate", ctypes.c_short),
                ("dmFormName", ctypes.c_wchar * 32),
                ("dmLogPixels", ctypes.c_ushort),
                ("dmBitsPerPel", ctypes.c_ulong),
                ("dmPelsWidth", ctypes.c_ulong),
                ("dmPelsHeight", ctypes.c_ulong),
                ("dmDisplayFlags", ctypes.c_ulong),
                ("dmDisplayFrequency", ctypes.c_ulong),
            ]

        devmode = DEVMODEW()
        devmode.dmSize = ctypes.sizeof(DEVMODEW)
        if ctypes.windll.user32.EnumDisplaySettingsW(None, -1, ctypes.byref(devmode)):
            if int(devmode.dmPelsWidth) > 0 and int(devmode.dmPelsHeight) > 0:
                OUTPUT_WIDTH = int(devmode.dmPelsWidth)
                OUTPUT_HEIGHT = int(devmode.dmPelsHeight)
            if int(devmode.dmDisplayFrequency) > 0:
                OUTPUT_FPS = int(devmode.dmDisplayFrequency)
    except Exception:
        pass


apply_native_display_settings()

# Apply user overrides after native display detection.
if CONFIG_RENDER_WIDTH > 0:
    OUTPUT_WIDTH = CONFIG_RENDER_WIDTH
if CONFIG_RENDER_HEIGHT > 0:
    OUTPUT_HEIGHT = CONFIG_RENDER_HEIGHT
if CONFIG_RENDER_FPS > 0:
    OUTPUT_FPS = CONFIG_RENDER_FPS

# Print path info early so non-technical users can see what was detected.
print("Config:", CONFIG_PATH)
print("Detected osu folder:", OSU_FOLDER or "not found")
print("Detected exports folder:", REPLAY_FOLDER or "not found")
print("Visual style:", VISUAL_STYLE)


# ============================================================
# DATA TYPES
# ============================================================


@dataclass
class ReplayFrame:
    t: int
    x: float
    y: float
    k1: bool
    k2: bool
    m1: bool
    m2: bool


@dataclass
class ClickEvent:
    t: int
    x: float
    y: float
    key: str


@dataclass
class TimingPoint:
    t: int
    beat_length: float
    inherited: bool
    sv_multiplier: float


@dataclass
class HitObject:
    x: int
    y: int
    t: int
    obj_type: int
    label: str
    index: int
    kind: str = "circle"   # "circle", "slider", "spinner"
    end_t: int = 0
    repeat_count: int = 1
    pixel_length: float = 0.0
    duration: int = 0
    curve_type: str = "B"
    control_points: Optional[List[Tuple[float, float]]] = None
    slider_path: Optional[List[Tuple[float, float]]] = None
    slider_tick_times: Optional[List[int]] = None


@dataclass
class ObjectJudgment:
    object_index: int
    t: int
    x: int
    y: int
    result: str            # "Great", "Ok", "Meh", "Miss"
    timing_error_ms: Optional[int] = None
    head_hit: bool = False
    slider_ticks_hit: int = 0
    slider_ticks_total: int = 0
    tail_hit: bool = True
    slider_break: bool = False
    judgment_t: Optional[int] = None
    resolved_t: Optional[int] = None


@dataclass
class BeatmapInfo:
    path: Path
    folder: Path
    title: str
    artist: str
    version: str
    creator: str
    audio_filename: str
    background_filename: str
    ar: float
    cs: float
    od: float
    slider_multiplier: float
    slider_tick_rate: float
    timing_points: List[TimingPoint]
    objects: List[HitObject]


@dataclass
class ReplayCandidate:
    path: Path
    timestamp_sort: float
    timestamp_text: str
    username: str
    beatmap_hash: str
    score: int
    misses: int


# ============================================================
# VIDEO WRITER
# ============================================================


class FFmpegVideoWriter:
    def __init__(self, path: str, width: int, height: int, fps: int):
        if imageio_ffmpeg is None:
            raise RuntimeError("imageio-ffmpeg is required. Install: pip install imageio-ffmpeg")

        self.path = path
        self.width = int(width)
        self.height = int(height)
        self.fps = int(fps)
        ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()

        if RENDER_ENCODER == "h264_nvenc":
            encoder_args = [
                "-c:v", "h264_nvenc",
                "-preset", NVENC_RENDER_PRESET,
                "-tune", "hq",
                "-rc", "constqp",
                "-qp", str(NVENC_RENDER_QP),
                "-pix_fmt", "yuv420p",
            ]
        elif RENDER_ENCODER == "libx264":
            encoder_args = [
                "-c:v", "libx264",
                "-preset", X264_RENDER_PRESET,
                "-crf", str(X264_RENDER_CRF),
                "-pix_fmt", "yuv420p",
            ]
        else:
            raise RuntimeError(f"Unknown RENDER_ENCODER: {RENDER_ENCODER}")

        cmd = [
            ffmpeg,
            "-y",
            "-hide_banner",
            "-loglevel", "warning",
            "-f", "rawvideo",
            "-vcodec", "rawvideo",
            "-pix_fmt", "bgr24",
            "-s", f"{self.width}x{self.height}",
            "-r", str(self.fps),
            "-i", "-",
            "-an",
        ]
        cmd.extend(encoder_args)
        cmd.extend(["-movflags", "+faststart", self.path])
        self.proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)

    def write(self, frame: np.ndarray) -> None:
        if frame.shape[1] != self.width or frame.shape[0] != self.height:
            raise RuntimeError(
                f"Frame size {frame.shape[1]}x{frame.shape[0]} does not match writer size {self.width}x{self.height}."
            )
        if self.proc.stdin is None:
            raise RuntimeError("FFmpeg stdin is closed.")
        self.proc.stdin.write(frame.tobytes())

    def release(self) -> None:
        if self.proc.stdin:
            self.proc.stdin.close()
        ret = self.proc.wait()
        if ret != 0:
            raise RuntimeError(f"FFmpeg video writer failed with exit code {ret}")


# ============================================================
# FILE DISCOVERY
# ============================================================


def find_osu_folder() -> Path:
    if OSU_FOLDER.strip():
        p = Path(OSU_FOLDER).expanduser()
        if p.exists():
            return p

    candidates = []
    appdata = os.environ.get("APPDATA")
    local = os.environ.get("LOCALAPPDATA")
    user = os.environ.get("USERPROFILE")

    if appdata:
        candidates.append(Path(appdata) / "osu")       # osu!lazer
    if local:
        candidates.append(Path(local) / "osu!")       # osu!stable
    if user:
        candidates.append(Path(user) / "AppData" / "Roaming" / "osu")
        candidates.append(Path(user) / "AppData" / "Local" / "osu!")

    candidates.append(Path.cwd())

    for p in candidates:
        if p.exists():
            return p

    raise FileNotFoundError("Could not auto-detect osu folder. Set OSU_FOLDER.")


def replay_timestamp_value(replay: Replay, fallback_mtime: float) -> Tuple[float, str]:
    ts = getattr(replay, "timestamp", None)
    if ts is None:
        return fallback_mtime, datetime.fromtimestamp(fallback_mtime).isoformat(timespec="seconds") + " file-mtime"

    try:
        if hasattr(ts, "timestamp"):
            value = float(ts.timestamp())
            return value, ts.isoformat(timespec="seconds")
    except Exception:
        pass

    try:
        value = float(ts)
        if value > 10_000_000_000:
            value /= 1000.0
        return value, datetime.fromtimestamp(value).isoformat(timespec="seconds")
    except Exception:
        return fallback_mtime, str(ts)


def collect_replay_candidates(osu_folder: Path) -> List[ReplayCandidate]:
    folders: List[Path] = []
    if REPLAY_FOLDER.strip():
        folders.append(Path(REPLAY_FOLDER).expanduser())
    folders.extend([osu_folder / "Replays", osu_folder / "Data" / "r"])

    paths: List[Path] = []
    for folder in folders:
        if folder.exists():
            paths.extend(folder.glob("*.osr"))

    paths = sorted(set(paths), key=lambda p: p.stat().st_mtime, reverse=True)[:MAX_REPLAY_CANDIDATES_TO_PARSE]

    candidates: List[ReplayCandidate] = []
    for path in paths:
        try:
            replay = Replay.from_path(path)
            mtime = path.stat().st_mtime
            sort_value, text = replay_timestamp_value(replay, mtime)
            candidates.append(ReplayCandidate(
                path=path,
                timestamp_sort=sort_value,
                timestamp_text=text,
                username=str(getattr(replay, "username", "unknown")),
                beatmap_hash=str(getattr(replay, "beatmap_hash", "")),
                score=int(getattr(replay, "score", 0)),
                misses=int(getattr(replay, "count_miss", 0)),
            ))
        except Exception:
            continue

    candidates.sort(key=lambda c: c.timestamp_sort, reverse=True)
    return candidates


def export_files_snapshot(folder: str, suffix: str) -> Dict[str, float]:
    p = Path(folder) if folder else Path()
    if not p.exists():
        return {}
    out: Dict[str, float] = {}
    for item in p.glob(f"*{suffix}"):
        if item.is_file():
            try:
                out[str(item.resolve())] = item.stat().st_mtime
            except OSError:
                pass
    return out


def newest_changed_file(before: Dict[str, float], after: Dict[str, float]) -> Optional[Path]:
    changed = []
    for path, mtime in after.items():
        if path not in before or mtime > before.get(path, 0):
            changed.append((mtime, path))
    if not changed:
        return None
    changed.sort(reverse=True)
    return Path(changed[0][1])


def print_missing_osz_export_steps(replay_path: Path) -> None:
    print()
    print("Could not find the matching .osz beatmap package yet.")
    print("The visualizer can wait while you export it from osu!lazer.")
    print()
    print("Step-by-step in osu!lazer:")
    print("  1. Go to the beatmap in song select.")
    print("  2. Right-click the difficulty for this replay:")
    print(f"     {replay_path.stem}")
    print("  3. Click Edit.")
    print("  4. In the editor, click File at the top-left.")
    print("  5. Click Export.")
    print("  6. Choose For compatibility (.osz).")
    print("  7. Wait for osu! to save the export.")
    print()
    print("The visualizer is watching your exports folder and will continue automatically.")
    print("Exports folder:", REPLAY_FOLDER)
    print()


def wait_for_user_exported_osz(replay_path: Path) -> Optional[Path]:
    if normalize_osu_install_type(OSU_INSTALL_TYPE) == "stable":
        print("Matching .osu beatmap was not found in the stable Songs folder.")
        print("Set the osu! root folder to your stable osu! install, or set specific beatmap .osu optional.")
        return None
    if not GUIDE_MISSING_OSZ_EXPORT:
        return None
    if not REPLAY_FOLDER or not Path(REPLAY_FOLDER).exists():
        print("Cannot guide .osz export because the exports folder was not found:", REPLAY_FOLDER)
        return None

    print_missing_osz_export_steps(replay_path)
    before_osz = export_files_snapshot(REPLAY_FOLDER, ".osz")
    last_status = 0.0

    while True:
        after_osz = export_files_snapshot(REPLAY_FOLDER, ".osz")
        new_osz = newest_changed_file(before_osz, after_osz)
        if new_osz:
            print("Detected new/updated .osz export:", new_osz)
            return new_osz

        now = time.time()
        if now - last_status >= 15.0:
            print("Still waiting for .osz export... export the beatmap as File > Export > For compatibility (.osz)")
            last_status = now
        time.sleep(1.0)


def wait_for_new_exports() -> Optional[Path]:
    if not REPLAY_FOLDER or not Path(REPLAY_FOLDER).exists():
        print("Watch mode could not start because the exports folder was not found:", REPLAY_FOLDER)
        return None

    before_osr = export_files_snapshot(REPLAY_FOLDER, ".osr")

    print()
    print("Watch mode is active.")
    print("Exports folder:", REPLAY_FOLDER)
    print("Now export/save a replay in osu!. The render will start when a new .osr appears.")
    print("If the matching beatmap .osz is missing, the tool will guide you and wait until you export it.")

    while True:
        time.sleep(1.0)
        after_osr = export_files_snapshot(REPLAY_FOLDER, ".osr")
        new_replay = newest_changed_file(before_osr, after_osr)
        if new_replay:
            print("Detected new replay:", new_replay)
            return new_replay


def find_replay(osu_folder: Path) -> Path:
    if REPLAY_PATH.strip():
        p = Path(REPLAY_PATH).expanduser()
        if p.exists():
            return p
        raise FileNotFoundError(f"REPLAY_PATH does not exist: {p}")

    candidates = collect_replay_candidates(osu_folder)
    if not candidates:
        raise FileNotFoundError("No readable .osr replays found. Export a replay or set REPLAY_PATH.")

    if PRINT_REPLAY_CANDIDATES:
        print("Newest replay candidates:")
        for i, c in enumerate(candidates[:5], start=1):
            print(f"  {i}. {c.timestamp_text} | user={c.username} | miss={c.misses} | score={c.score} | {c.path.name}")

    return candidates[0].path


def md5_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def md5_file(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_extract_zip(zip_path: Path, target_dir: Path) -> None:
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            name = member.filename.replace("\\", "/")
            if name.startswith("/") or ".." in Path(name).parts:
                continue
            zf.extract(member, target_dir)


def norm_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def replay_name_hints(replay_path: Path) -> Tuple[str, str]:
    stem = replay_path.stem

    if " playing " in stem:
        stem = stem.split(" playing ", 1)[1]

    stem = re.sub(r"\s*\(\d{4}-\d{2}-\d{2}[_\- ].*?\)\s*$", "", stem)

    brackets = re.findall(r"\[([^\]]+)\]", stem)
    difficulty = brackets[-1] if brackets else ""
    songish = re.sub(r"\[[^\]]+\]", "", stem)

    return norm_text(songish), norm_text(difficulty)


def osu_metadata_from_bytes(data: bytes) -> Dict[str, str]:
    text = data.decode("utf-8", errors="ignore")
    section = ""
    meta: Dict[str, str] = {}

    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("//"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1]
            continue
        if section == "Metadata" and ":" in line:
            k, v = line.split(":", 1)
            meta[k.strip()] = v.strip()

    return meta


def osz_files_to_search() -> List[Path]:
    folders: List[Path] = []

    for folder in OSZ_SEARCH_FOLDERS:
        if folder and str(folder).strip():
            folders.append(Path(folder).expanduser())

    if REPLAY_FOLDER.strip():
        folders.append(Path(REPLAY_FOLDER).expanduser())

    seen = set()
    files: List[Path] = []

    for folder in folders:
        key = str(folder.resolve()) if folder.exists() else str(folder)
        if key in seen:
            continue
        seen.add(key)

        if not folder.exists():
            continue

        for p in folder.iterdir():
            if p.is_file() and p.suffix.lower() in (".osz", ".zip"):
                files.append(p)

    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def extract_matching_osu_from_osz(osz_path: Path, member: str, beatmap_hash: str) -> Optional[Path]:
    extract_dir = Path(EXTRACTED_OSZ_DIR) / (osz_path.stem + "_" + beatmap_hash[:8])
    print(f"Matched exported beatmap package: {osz_path}")

    safe_extract_zip(osz_path, extract_dir)

    matched_path = extract_dir / member
    if matched_path.exists():
        return matched_path

    for p in extract_dir.rglob("*.osu"):
        if p.name == Path(member).name:
            return p

    return None


def find_beatmap_in_osz_exports(beatmap_hash: str, replay_path: Path) -> Optional[Path]:
    target = beatmap_hash.lower()
    song_hint, diff_hint = replay_name_hints(replay_path)
    osz_files = osz_files_to_search()

    if not osz_files:
        return None

    print("Searching exported .osz packages for matching beatmap...")
    print(f"Replay song hint: {song_hint or '(none)'}")
    print(f"Replay diff hint: {diff_hint or '(none)'}")

    fallback_best = None
    fallback_score = -1

    for osz_path in osz_files:
        try:
            with zipfile.ZipFile(osz_path, "r") as zf:
                osu_members = [m for m in zf.namelist() if m.lower().endswith(".osu")]

                for member in osu_members:
                    data = zf.read(member)

                    if md5_bytes(data).lower() == target:
                        return extract_matching_osu_from_osz(osz_path, member, beatmap_hash)

                    meta = osu_metadata_from_bytes(data)
                    version = norm_text(meta.get("Version", ""))
                    member_norm = norm_text(Path(member).stem)
                    osz_norm = norm_text(osz_path.stem)
                    title_norm = norm_text(meta.get("Artist", "") + " " + meta.get("Title", ""))

                    score = 0

                    if diff_hint and (diff_hint == version or diff_hint in member_norm):
                        score += 100
                    if song_hint and (
                        song_hint in osz_norm
                        or osz_norm in song_hint
                        or title_norm in song_hint
                        or song_hint in title_norm
                    ):
                        score += 40
                    if diff_hint and diff_hint in member_norm:
                        score += 25

                    if score > fallback_score:
                        fallback_score = score
                        fallback_best = (osz_path, member)

        except Exception as exc:
            print(f"  skipped {osz_path.name}: {exc}")
            continue

    if fallback_best and fallback_score >= 80:
        osz_path, member = fallback_best
        print(f"No exact hash match; using filename/difficulty fallback with score {fallback_score}.")
        return extract_matching_osu_from_osz(osz_path, member, beatmap_hash)

    print("No exported .osz matched by hash or replay filename/difficulty.")
    return None


def find_beatmap(osu_folder: Path, beatmap_hash: str, replay_path: Path) -> Optional[Path]:
    if BEATMAP_PATH.strip():
        p = Path(BEATMAP_PATH).expanduser()
        if p.exists():
            return p
        raise FileNotFoundError(f"BEATMAP_PATH does not exist: {p}")

    songs = osu_folder / "Songs"
    if songs.exists():
        target = beatmap_hash.lower()
        print("Searching stable Songs folder for matching .osu beatmap hash...")
        for checked, osu_path in enumerate(songs.rglob("*.osu"), start=1):
            try:
                if md5_file(osu_path).lower() == target:
                    print(f"Matched beatmap after checking {checked} files.")
                    return osu_path
            except OSError:
                continue
            if checked % 1000 == 0:
                print(f"  checked {checked} .osu files...")

    return find_beatmap_in_osz_exports(beatmap_hash, replay_path)


def find_file_case_insensitive(folder: Path, filename: str) -> Optional[Path]:
    if not filename:
        return None

    direct = folder / filename
    if direct.exists():
        return direct

    target = filename.lower().replace("\\", "/")

    for p in folder.rglob("*"):
        if p.is_file():
            rel = str(p.relative_to(folder)).lower().replace("\\", "/")
            if rel == target or p.name.lower() == Path(filename).name.lower():
                return p

    return None


def find_audio_file(beatmap: Optional[BeatmapInfo]) -> Optional[Path]:
    if beatmap is None:
        return None

    exact = find_file_case_insensitive(beatmap.folder, beatmap.audio_filename)
    if exact and exact.exists():
        return exact

    for ext in ("*.mp3", "*.ogg", "*.wav", "*.flac", "*.m4a"):
        files = list(beatmap.folder.rglob(ext))
        if files:
            return files[0]

    return None


# ============================================================
# GEOMETRY / SLIDER HELPERS
# ============================================================


def clamp01(u: float) -> float:
    return max(0.0, min(1.0, float(u)))


def distance(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    return math.hypot(a[0] - b[0], a[1] - b[1])


def polyline_length(points: List[Tuple[float, float]]) -> float:
    if len(points) < 2:
        return 0.0
    return sum(distance(points[i - 1], points[i]) for i in range(1, len(points)))


def point_at_distance(points: List[Tuple[float, float]], d: float) -> Tuple[float, float]:
    if not points:
        return 0.0, 0.0
    if len(points) == 1:
        return points[0]

    if d <= 0:
        return points[0]

    remaining = d
    for i in range(1, len(points)):
        a = points[i - 1]
        b = points[i]
        seg = distance(a, b)
        if seg <= 0:
            continue
        if remaining <= seg:
            u = remaining / seg
            return a[0] + (b[0] - a[0]) * u, a[1] + (b[1] - a[1]) * u
        remaining -= seg

    return points[-1]


def point_at_fraction(points: List[Tuple[float, float]], u: float) -> Tuple[float, float]:
    u = clamp01(u)
    total = polyline_length(points)
    if total <= 0:
        return points[0] if points else (0.0, 0.0)
    return point_at_distance(points, total * u)


def truncate_path(points: List[Tuple[float, float]], target_len: float) -> List[Tuple[float, float]]:
    if len(points) < 2 or target_len <= 0:
        return points

    total = polyline_length(points)
    if total <= target_len:
        return points

    out = [points[0]]
    remaining = target_len
    for i in range(1, len(points)):
        a = points[i - 1]
        b = points[i]
        seg = distance(a, b)
        if seg <= 0:
            continue
        if remaining <= seg:
            u = remaining / seg
            out.append((a[0] + (b[0] - a[0]) * u, a[1] + (b[1] - a[1]) * u))
            return out
        out.append(b)
        remaining -= seg

    return out


def dedupe_points(points: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
    out: List[Tuple[float, float]] = []
    for p in points:
        if not out or distance(out[-1], p) > 0.01:
            out.append(p)
    return out if out else points[:1]


def slice_path_by_fraction(points: List[Tuple[float, float]], u0: float, u1: float) -> List[Tuple[float, float]]:
    if not points:
        return []
    if len(points) == 1:
        return points[:]

    u0 = clamp01(u0)
    u1 = clamp01(u1)
    if abs(u1 - u0) < 1e-6:
        return [point_at_fraction(points, u0)]

    reverse = u1 < u0
    a, b = (u1, u0) if reverse else (u0, u1)

    total = polyline_length(points)
    if total <= 0:
        return points[:]

    d0 = total * a
    d1 = total * b
    out = [point_at_distance(points, d0)]
    acc = 0.0

    for i in range(1, len(points)):
        seg = distance(points[i - 1], points[i])
        if seg <= 0:
            continue
        next_acc = acc + seg
        if d0 < next_acc < d1:
            out.append(points[i])
        acc = next_acc

    out.append(point_at_distance(points, d1))
    out = dedupe_points(out)
    if reverse:
        out.reverse()
    return out


def bezier_point(points: List[Tuple[float, float]], u: float) -> Tuple[float, float]:
    tmp = [(float(x), float(y)) for x, y in points]
    n = len(tmp)
    for r in range(1, n):
        for i in range(n - r):
            tmp[i] = (
                tmp[i][0] * (1.0 - u) + tmp[i + 1][0] * u,
                tmp[i][1] * (1.0 - u) + tmp[i + 1][1] * u,
            )
    return tmp[0]


def sample_bezier(points: List[Tuple[float, float]], samples: int = 60) -> List[Tuple[float, float]]:
    if len(points) < 2:
        return points
    return [bezier_point(points, i / float(samples)) for i in range(samples + 1)]


def split_bezier_segments(points: List[Tuple[float, float]]) -> List[List[Tuple[float, float]]]:
    if len(points) < 2:
        return [points]

    segments = []
    current = [points[0]]
    for p in points[1:]:
        if current and distance(current[-1], p) < 0.001:
            if len(current) >= 2:
                segments.append(current)
            current = [p]
        else:
            current.append(p)
    if len(current) >= 2:
        segments.append(current)
    return segments if segments else [points]


def sample_catmull(points: List[Tuple[float, float]], samples_per_seg: int = 24) -> List[Tuple[float, float]]:
    if len(points) < 2:
        return points
    out = []
    pts = [points[0]] + points + [points[-1]]
    for i in range(1, len(pts) - 2):
        p0, p1, p2, p3 = pts[i - 1], pts[i], pts[i + 1], pts[i + 2]
        for j in range(samples_per_seg):
            t = j / float(samples_per_seg)
            t2 = t * t
            t3 = t2 * t
            x = 0.5 * ((2 * p1[0]) + (-p0[0] + p2[0]) * t + (2*p0[0] - 5*p1[0] + 4*p2[0] - p3[0]) * t2 + (-p0[0] + 3*p1[0] - 3*p2[0] + p3[0]) * t3)
            y = 0.5 * ((2 * p1[1]) + (-p0[1] + p2[1]) * t + (2*p0[1] - 5*p1[1] + 4*p2[1] - p3[1]) * t2 + (-p0[1] + 3*p1[1] - 3*p2[1] + p3[1]) * t3)
            out.append((x, y))
    out.append(points[-1])
    return out


def sample_perfect_arc(points: List[Tuple[float, float]], samples: int = 80) -> List[Tuple[float, float]]:
    if len(points) < 3:
        return points
    p1, p2, p3 = points[0], points[1], points[2]
    x1, y1 = p1
    x2, y2 = p2
    x3, y3 = p3

    d = 2 * (x1*(y2-y3) + x2*(y3-y1) + x3*(y1-y2))
    if abs(d) < 1e-6:
        return points

    ux = ((x1*x1 + y1*y1)*(y2-y3) + (x2*x2 + y2*y2)*(y3-y1) + (x3*x3 + y3*y3)*(y1-y2)) / d
    uy = ((x1*x1 + y1*y1)*(x3-x2) + (x2*x2 + y2*y2)*(x1-x3) + (x3*x3 + y3*y3)*(x2-x1)) / d
    r = math.hypot(x1 - ux, y1 - uy)
    if r <= 0:
        return points

    a1 = math.atan2(y1 - uy, x1 - ux)
    a2 = math.atan2(y2 - uy, x2 - ux)
    a3 = math.atan2(y3 - uy, x3 - ux)

    def norm(a):
        while a < 0:
            a += 2 * math.pi
        while a >= 2 * math.pi:
            a -= 2 * math.pi
        return a

    n1, n2, n3 = norm(a1), norm(a2), norm(a3)
    ccw_span = (n3 - n1) % (2 * math.pi)
    mid_span = (n2 - n1) % (2 * math.pi)
    ccw = mid_span <= ccw_span

    if ccw:
        span = ccw_span
    else:
        span = -((n1 - n3) % (2 * math.pi))

    return [(ux + math.cos(a1 + span * (i / samples)) * r, uy + math.sin(a1 + span * (i / samples)) * r) for i in range(samples + 1)]


def compute_slider_path(curve_type: str, points: List[Tuple[float, float]], pixel_length: float) -> List[Tuple[float, float]]:
    if len(points) < 2:
        return points

    curve_type = (curve_type or "B").upper()

    if curve_type == "L":
        path = points
    elif curve_type == "C":
        path = sample_catmull(points)
    elif curve_type == "P":
        path = sample_perfect_arc(points[:3]) if len(points) >= 3 else points
        if len(points) > 3:
            path.extend(points[3:])
    else:
        path = []
        for seg in split_bezier_segments(points):
            sampled = sample_bezier(seg)
            if path and sampled:
                sampled = sampled[1:]
            path.extend(sampled)
        if not path:
            path = points

    return truncate_path(path, pixel_length)


def timing_at(t: int, timing_points: List[TimingPoint]) -> Tuple[float, float]:
    beat_length = 500.0
    sv_multiplier = 1.0
    for tp in timing_points:
        if tp.t > t:
            break
        if tp.inherited:
            sv_multiplier = tp.sv_multiplier
        else:
            beat_length = tp.beat_length
    return beat_length, sv_multiplier


def slider_repeat_state(obj: HitObject, song_t: int) -> Tuple[int, float, bool]:
    if obj.duration <= 0 or obj.repeat_count <= 0:
        return 0, 0.0, False

    elapsed = max(0.0, min(float(obj.duration), float(song_t - obj.t)))
    segment = obj.duration / float(max(1, obj.repeat_count))
    if segment <= 0:
        return 0, 0.0, False

    repeat_idx = int(min(obj.repeat_count - 1, math.floor(elapsed / segment)))
    local = elapsed - repeat_idx * segment
    linear_u = clamp01(local / segment)
    reverse = repeat_idx % 2 == 1
    path_u = 1.0 - linear_u if reverse else linear_u
    return repeat_idx, path_u, reverse


def slider_position(obj: HitObject, song_t: int) -> Tuple[float, float]:
    if not obj.slider_path:
        return float(obj.x), float(obj.y)
    _, path_u, _ = slider_repeat_state(obj, song_t)
    return point_at_fraction(obj.slider_path, path_u)


def finalize_slider_objects(objects: List[HitObject], timing_points: List[TimingPoint], slider_multiplier: float, slider_tick_rate: float) -> None:
    for obj in objects:
        if obj.kind != "slider":
            obj.duration = 0
            if obj.end_t <= 0:
                obj.end_t = obj.t
            continue

        if not obj.control_points or len(obj.control_points) < 2:
            obj.duration = 0
            obj.end_t = obj.t
            obj.slider_path = [(obj.x, obj.y)]
            obj.slider_tick_times = []
            continue

        obj.slider_path = compute_slider_path(obj.curve_type, obj.control_points, obj.pixel_length)
        beat_length, sv_multiplier = timing_at(obj.t, timing_points)
        px_per_beat = max(1e-6, slider_multiplier * 100.0 * sv_multiplier)
        segment_duration = obj.pixel_length / px_per_beat * beat_length
        total_duration = int(round(segment_duration * max(1, obj.repeat_count)))
        obj.duration = max(0, total_duration)
        obj.end_t = obj.t + obj.duration

        ticks: List[int] = []
        tick_rate = max(0.1, slider_tick_rate)
        tick_distance = px_per_beat / tick_rate
        if tick_distance > 0 and obj.pixel_length > 0 and segment_duration > 0:
            for repeat in range(max(1, obj.repeat_count)):
                d = tick_distance
                while d < obj.pixel_length - 1.0:
                    local_time = (d / obj.pixel_length) * segment_duration
                    tick_t = int(round(obj.t + repeat * segment_duration + local_time))
                    ticks.append(tick_t)
                    d += tick_distance
        obj.slider_tick_times = ticks


# ============================================================
# PARSING
# ============================================================


def key_mask(keys) -> int:
    if hasattr(keys, "value"):
        return int(keys.value)
    try:
        return int(keys)
    except Exception:
        return 0


def parse_replay_frames(replay: Replay) -> Tuple[List[ReplayFrame], List[ClickEvent]]:
    frames: List[ReplayFrame] = []
    keyboard_clicks: List[ClickEvent] = []
    mouse_clicks: List[ClickEvent] = []

    t = 0
    prev_k1 = False
    prev_k2 = False
    prev_m1 = False
    prev_m2 = False

    for ev in replay.replay_data:
        dt = int(getattr(ev, "time_delta", 0))
        if dt < 0:
            continue

        t += dt

        if not hasattr(ev, "x") or not hasattr(ev, "y"):
            continue

        x = float(getattr(ev, "x"))
        y = float(getattr(ev, "y"))
        mask = key_mask(getattr(ev, "keys", 0))

        m1 = bool(mask & 1)
        m2 = bool(mask & 2)
        k1 = bool(mask & 4)
        k2 = bool(mask & 8)

        frames.append(ReplayFrame(t=t, x=x, y=y, k1=k1, k2=k2, m1=m1, m2=m2))

        if k1 and not prev_k1:
            keyboard_clicks.append(ClickEvent(t=t, x=x, y=y, key=LABEL_K1))
        if k2 and not prev_k2:
            keyboard_clicks.append(ClickEvent(t=t, x=x, y=y, key=LABEL_K2))

        if m1 and not prev_m1:
            mouse_clicks.append(ClickEvent(t=t, x=x, y=y, key=LABEL_K1))
        if m2 and not prev_m2:
            mouse_clicks.append(ClickEvent(t=t, x=x, y=y, key=LABEL_K2))

        prev_k1 = k1
        prev_k2 = k2
        prev_m1 = m1
        prev_m2 = m2

    if keyboard_clicks:
        return frames, keyboard_clicks

    if USE_MOUSE_BUTTON_FALLBACK_FOR_CLICKS and mouse_clicks:
        print("No K1/K2 keyboard taps found; using M1/M2 replay presses as A/D fallback.")
        return frames, mouse_clicks

    return frames, keyboard_clicks


def parse_float(value: str, default: float) -> float:
    try:
        return float(value.strip())
    except Exception:
        return default


def strip_quotes(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and ((s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'")):
        return s[1:-1]
    return s


def parse_beatmap(path: Path) -> BeatmapInfo:
    section = ""
    general: Dict[str, str] = {}
    meta: Dict[str, str] = {}
    diff: Dict[str, str] = {}
    background = ""
    timing_points: List[TimingPoint] = []
    objects: List[HitObject] = []
    index = 1

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("//"):
                continue

            if line.startswith("[") and line.endswith("]"):
                section = line[1:-1]
                continue

            if section in ("General", "Metadata", "Difficulty") and ":" in line:
                k, v = line.split(":", 1)
                if section == "General":
                    general[k.strip()] = v.strip()
                elif section == "Metadata":
                    meta[k.strip()] = v.strip()
                else:
                    diff[k.strip()] = v.strip()

            elif section == "TimingPoints":
                parts = line.split(",")
                if len(parts) >= 2:
                    try:
                        tp_t = int(round(float(parts[0])))
                        beat_len = float(parts[1])
                    except ValueError:
                        continue
                    uninherited = True
                    if len(parts) >= 7:
                        uninherited = parts[6].strip() == "1"
                    inherited = not uninherited
                    if inherited and beat_len != 0:
                        sv = max(0.01, -100.0 / beat_len)
                    else:
                        sv = 1.0
                    timing_points.append(TimingPoint(tp_t, abs(beat_len) if not inherited else beat_len, inherited, sv))

            elif section == "Events":
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 3 and parts[0] in ("0", "Video", "1"):
                    candidate = strip_quotes(parts[2])
                    if candidate.lower().endswith((".jpg", ".jpeg", ".png", ".bmp")):
                        background = candidate

            elif section == "HitObjects":
                parts = line.split(",")
                if len(parts) >= 4:
                    try:
                        x = int(parts[0])
                        y = int(parts[1])
                        tt = int(parts[2])
                        typ = int(parts[3])
                    except ValueError:
                        continue

                    kind = "circle"
                    end_t = tt
                    repeat_count = 1
                    pixel_length = 0.0
                    curve_type = "B"
                    control_points: Optional[List[Tuple[float, float]]] = None

                    if typ & 2:
                        kind = "slider"
                        curve = parts[5] if len(parts) > 5 else ""
                        if len(parts) > 6:
                            try:
                                repeat_count = max(1, int(parts[6]))
                            except ValueError:
                                repeat_count = 1
                        if len(parts) > 7:
                            try:
                                pixel_length = max(0.0, float(parts[7]))
                            except ValueError:
                                pixel_length = 0.0

                        pts: List[Tuple[float, float]] = [(float(x), float(y))]
                        if curve:
                            curve_parts = curve.split("|")
                            if curve_parts:
                                curve_type = curve_parts[0].strip() or "B"
                            for token in curve_parts[1:]:
                                if ":" in token:
                                    sx, sy = token.split(":", 1)
                                    try:
                                        pts.append((float(sx), float(sy)))
                                    except ValueError:
                                        pass
                        control_points = pts

                    elif typ & 8:
                        kind = "spinner"
                        if len(parts) > 5:
                            try:
                                end_t = int(parts[5])
                            except ValueError:
                                end_t = tt

                    if kind == "slider":
                        label = "S"
                    elif kind == "spinner":
                        label = "SP"
                    else:
                        label = str(index)

                    objects.append(HitObject(
                        x=x,
                        y=y,
                        t=tt,
                        obj_type=typ,
                        label=label,
                        index=index,
                        kind=kind,
                        end_t=end_t,
                        repeat_count=repeat_count,
                        pixel_length=pixel_length,
                        duration=0,
                        curve_type=curve_type,
                        control_points=control_points,
                        slider_path=None,
                        slider_tick_times=[],
                    ))
                    index += 1

    ar = parse_float(diff.get("ApproachRate", diff.get("OverallDifficulty", "9")), 9.0)
    cs = parse_float(diff.get("CircleSize", "4"), 4.0)
    od = parse_float(diff.get("OverallDifficulty", "8"), 8.0)
    slider_multiplier = parse_float(diff.get("SliderMultiplier", "1.4"), 1.4)
    slider_tick_rate = parse_float(diff.get("SliderTickRate", "1"), 1.0)

    timing_points.sort(key=lambda p: p.t)
    if not timing_points:
        timing_points = [TimingPoint(0, 500.0, False, 1.0)]

    finalize_slider_objects(objects, timing_points, slider_multiplier, slider_tick_rate)

    return BeatmapInfo(
        path=path,
        folder=path.parent,
        title=meta.get("Title", path.stem),
        artist=meta.get("Artist", "Unknown Artist"),
        version=meta.get("Version", "Unknown Difficulty"),
        creator=meta.get("Creator", "Unknown Creator"),
        audio_filename=general.get("AudioFilename", ""),
        background_filename=background,
        ar=ar,
        cs=cs,
        od=od,
        slider_multiplier=slider_multiplier,
        slider_tick_rate=slider_tick_rate,
        timing_points=timing_points,
        objects=objects,
    )


# ============================================================
# HIT JUDGMENT HELPERS
# ============================================================


def hit_windows_ms(od: float) -> Tuple[int, int, int]:
    w300 = int(round(79.5 - 6 * od))
    w100 = int(round(139.5 - 8 * od))
    w50 = int(round(199.5 - 10 * od))
    return max(0, w300), max(0, w100), max(0, w50)


def judgment_color(result: str) -> Tuple[int, int, int]:
    return {
        "Great": COLOR_GREAT,
        "Ok": COLOR_OK,
        "Meh": COLOR_MEH,
        "Miss": COLOR_MISS,
    }.get(result, COLOR_TEXT)


def judgment_label(result: str) -> str:
    return JUDGMENT_TEXT_LABELS.get(result, result)


def frame_at_song_time_for_judgment(frames: List[ReplayFrame], times: List[int], song_t: int, replay_to_song_offset: int) -> Optional[ReplayFrame]:
    if not frames:
        return None
    replay_t = song_t - replay_to_song_offset
    idx = bisect.bisect_left(times, replay_t)
    if idx <= 0:
        return frames[0]
    if idx >= len(frames):
        return frames[-1]
    a = frames[idx - 1]
    b = frames[idx]
    span = max(1, b.t - a.t)
    u = (replay_t - a.t) / span
    return ReplayFrame(
        song_t,
        a.x + (b.x - a.x) * u,
        a.y + (b.y - a.y) * u,
        a.k1 if u < 0.5 else b.k1,
        a.k2 if u < 0.5 else b.k2,
        a.m1 if u < 0.5 else b.m1,
        a.m2 if u < 0.5 else b.m2,
    )


def frame_is_held(frame: Optional[ReplayFrame]) -> bool:
    if frame is None:
        return False
    return frame.k1 or frame.k2 or frame.m1 or frame.m2


def judge_hit_objects(
    objects: List[HitObject],
    clicks: List[ClickEvent],
    click_song_times: List[int],
    frames: List[ReplayFrame],
    replay_to_song_offset: int,
    cs: float,
    od: float,
) -> Dict[int, ObjectJudgment]:
    judged: Dict[int, ObjectJudgment] = {}
    if not objects:
        return judged

    w300, w100, w50 = hit_windows_ms(od)

    if not clicks:
        for obj in objects:
            if obj.kind in ("circle", "slider"):
                miss_t = obj.t + w50
                resolved_t = miss_t if obj.kind == "circle" else obj.end_t
                judged[obj.index] = ObjectJudgment(
                    object_index=obj.index,
                    t=obj.t,
                    x=obj.x,
                    y=obj.y,
                    result="Miss",
                    timing_error_ms=None,
                    head_hit=False,
                    slider_ticks_hit=0,
                    slider_ticks_total=len(obj.slider_tick_times or []) if obj.kind == "slider" else 0,
                    tail_hit=False if obj.kind == "slider" else True,
                    slider_break=False,
                    judgment_t=miss_t,
                    resolved_t=resolved_t,
                )
        return judged

    times = [f.t for f in frames]
    base_radius_pf = 54.4 - 4.48 * cs
    head_radius_pf = max(8.0, base_radius_pf * CIRCLE_HIT_RADIUS_MULTIPLIER)
    slider_radius_pf = max(8.0, base_radius_pf * SLIDER_BODY_HIT_RADIUS_MULTIPLIER)
    used_clicks = set()

    for obj in objects:
        if obj.kind not in ("circle", "slider"):
            continue

        left = bisect.bisect_left(click_song_times, obj.t - w50)
        right = bisect.bisect_right(click_song_times, obj.t + w50)
        best_idx = None
        best_key = None

        # Main judgment: match only the slider/circle head click.
        for idx in range(left, right):
            if idx in used_clicks:
                continue
            c = clicks[idx]
            dist = math.hypot(c.x - obj.x, c.y - obj.y)
            if dist > head_radius_pf:
                continue
            dt = int(round(click_song_times[idx] - obj.t))
            key = (abs(dt), dist)
            if best_key is None or key < best_key:
                best_key = key
                best_idx = idx

        if best_idx is None:
            miss_t = obj.t + w50
            resolved_t = miss_t if obj.kind == "circle" else obj.end_t
            judged[obj.index] = ObjectJudgment(
                object_index=obj.index,
                t=obj.t,
                x=obj.x,
                y=obj.y,
                result="Miss",
                timing_error_ms=None,
                head_hit=False,
                slider_ticks_hit=0,
                slider_ticks_total=len(obj.slider_tick_times or []) if obj.kind == "slider" else 0,
                tail_hit=False if obj.kind == "slider" else True,
                slider_break=False,
                judgment_t=miss_t,
                resolved_t=resolved_t,
            )
            continue

        used_clicks.add(best_idx)
        hit_song_t = click_song_times[best_idx]
        err = int(round(hit_song_t - obj.t))
        abs_err = abs(err)

        # Head result controls the object/slider color.
        if abs_err <= w300:
            head_result = "Great"
        elif abs_err <= w100:
            head_result = "Ok"
        elif abs_err <= w50:
            head_result = "Meh"
        else:
            head_result = "Miss"

        ticks_hit = 0
        ticks_total = 0
        tail_hit = True
        slider_break = False

        if obj.kind == "slider":
            tick_times = obj.slider_tick_times or []
            ticks_total = len(tick_times)

            for tick_t in tick_times:
                frame = frame_at_song_time_for_judgment(frames, times, tick_t, replay_to_song_offset)
                target = slider_position(obj, tick_t)
                if (
                    frame_is_held(frame)
                    and frame is not None
                    and math.hypot(frame.x - target[0], frame.y - target[1]) <= slider_radius_pf
                ):
                    ticks_hit += 1

            tail_frame = frame_at_song_time_for_judgment(frames, times, obj.end_t, replay_to_song_offset)
            tail_target = slider_position(obj, obj.end_t)
            tail_hit = bool(
                frame_is_held(tail_frame)
                and tail_frame is not None
                and math.hypot(tail_frame.x - tail_target[0], tail_frame.y - tail_target[1]) <= slider_radius_pf
            )

            # Separate slider-follow status from the main head result.
            slider_break = (ticks_hit < ticks_total) or (not tail_hit)

            # Head judgment happens at the head, but slider remains visible until end.
            judgment_t = hit_song_t if head_result != "Miss" else (obj.t + w50)
            resolved_t = obj.end_t
        else:
            judgment_t = hit_song_t if head_result != "Miss" else (obj.t + w50)
            resolved_t = judgment_t

        judged[obj.index] = ObjectJudgment(
            object_index=obj.index,
            t=obj.t,
            x=obj.x,
            y=obj.y,
            result=head_result,
            timing_error_ms=err,
            head_hit=(head_result != "Miss"),
            slider_ticks_hit=ticks_hit,
            slider_ticks_total=ticks_total,
            tail_hit=tail_hit,
            slider_break=slider_break,
            judgment_t=judgment_t,
            resolved_t=resolved_t,
        )

    return judged


# ============================================================
# TIMING CALIBRATION
# ============================================================


def nearest_abs_distance(value: int, sorted_values: List[int]) -> int:
    idx = bisect.bisect_left(sorted_values, value)
    best = 10**9
    if idx < len(sorted_values):
        best = min(best, abs(sorted_values[idx] - value))
    if idx > 0:
        best = min(best, abs(sorted_values[idx - 1] - value))
    return best


def estimate_replay_to_song_offset(clicks: List[ClickEvent], objects: List[HitObject]) -> int:
    if not AUTO_CALIBRATE_TIMING or not USE_AUTO_OFFSET_PLUS_MANUAL or not clicks or not objects:
        return MANUAL_REPLAY_TO_SONG_OFFSET_MS

    click_times = [c.t for c in clicks[:TIMING_CALIBRATION_MAX_CLICKS]]
    object_times = sorted(o.t for o in objects[:TIMING_CALIBRATION_MAX_OBJECTS] if o.kind in ("circle", "slider"))

    best_offset = 0
    best_score = float("inf")
    best_hits = -1

    for offset in range(-TIMING_CALIBRATION_SEARCH_MS, TIMING_CALIBRATION_SEARCH_MS + 1, TIMING_CALIBRATION_STEP_MS):
        distances = [nearest_abs_distance(t + offset, object_times) for t in click_times]
        hits = sum(1 for d in distances if d <= TIMING_HIT_WINDOW_MS)
        median = float(np.median(distances))
        score = median - hits * 1.5

        if score < best_score or (abs(score - best_score) < 0.001 and hits > best_hits):
            best_score = score
            best_offset = offset
            best_hits = hits

    total = best_offset + MANUAL_REPLAY_TO_SONG_OFFSET_MS
    print(f"Auto timing calibration: offset={best_offset}ms, matched_taps≈{best_hits}/{len(click_times)}")
    print(f"Manual timing fine tune: {MANUAL_REPLAY_TO_SONG_OFFSET_MS}ms -> total replay_to_song_offset={total}ms")
    return total


# ============================================================
# RENDERING
# ============================================================


class Renderer:
    def __init__(self, frames: List[ReplayFrame], clicks: List[ClickEvent], beatmap: Optional[BeatmapInfo], replay: Replay, replay_to_song_offset: int, replay_path: Optional[Path] = None):
        self.frames = frames
        self.clicks = clicks
        self.beatmap = beatmap
        self.replay = replay
        self.replay_to_song_offset = replay_to_song_offset
        self.replay_path = replay_path
        self.audio_path = find_audio_file(beatmap)

        self.times = [f.t for f in frames]
        self.click_song_times = [c.t + replay_to_song_offset for c in clicks]
        self.objects = beatmap.objects if beatmap else []
        self.object_times = [o.t for o in self.objects]
        self.hit_window_300, self.hit_window_100, self.hit_window_50 = hit_windows_ms(beatmap.od if beatmap else 8.0)
        self.judgments = (
            judge_hit_objects(
                self.objects,
                self.clicks,
                self.click_song_times,
                self.frames,
                replay_to_song_offset,
                beatmap.cs if beatmap else 4.0,
                beatmap.od if beatmap else 8.0,
            )
            if beatmap
            else {}
        )
        self.judgment_events_sorted = sorted(
            (
                j for j in self.judgments.values()
                if j.result in ("Great", "Ok", "Meh", "Miss") and j.resolved_t is not None
            ),
            key=lambda j: int(j.resolved_t),
        )
        self.judgment_totals = {"Great": 0, "Ok": 0, "Meh": 0, "Miss": 0}
        self.judgment_totals_idx = 0
        self.judgment_totals_last_t: Optional[int] = None

        if USE_OSU_SCREENSPACE_MAPPING:
            self.scale = OUTPUT_HEIGHT / 480.0
            self.field_w = int(round(512 * self.scale))
            self.field_h = int(round(384 * self.scale))
            self.origin_x = int(round((OUTPUT_WIDTH - 640 * self.scale) / 2.0 + 64 * self.scale))
            self.origin_y = int(round(48 * self.scale))
        else:
            self.scale = min((OUTPUT_WIDTH - 140) / 512.0, (OUTPUT_HEIGHT - 150) / 384.0)
            self.field_w = int(round(512 * self.scale))
            self.field_h = int(round(384 * self.scale))
            self.origin_x = (OUTPUT_WIDTH - self.field_w) // 2
            self.origin_y = 74

        cs = beatmap.cs if beatmap else 4.0
        self.circle_radius = max(1, int(round((54.4 - 4.48 * cs) * self.scale)))

        ar = beatmap.ar if beatmap else 9.0
        if ar < 5:
            self.preempt = int(1800 - 120 * ar)
        else:
            self.preempt = int(1200 - 150 * (ar - 5))
        self.preempt = max(450, min(1800, self.preempt))

        self.visual_style = VISUAL_STYLE
        if self.visual_style == "ghost":
            self.circle_fill_alpha = GHOST_CIRCLE_ALPHA
            self.slider_fill_alpha = GHOST_SLIDER_ALPHA
            self.slider_ball_alpha = GHOST_SLIDER_BALL_ALPHA
            self.follow_circle_alpha = GHOST_FOLLOW_CIRCLE_ALPHA
        else:
            self.circle_fill_alpha = 1.0
            self.slider_fill_alpha = 1.0
            self.slider_ball_alpha = 1.0
            self.follow_circle_alpha = 1.0

        self.background = self.load_background()
        self.base_template = self.make_base_template()

    def load_background(self) -> Optional[np.ndarray]:
        if not DRAW_BACKGROUND or not self.beatmap or not self.beatmap.background_filename:
            return None

        path = find_file_case_insensitive(self.beatmap.folder, self.beatmap.background_filename)
        if not path or not path.exists():
            return None

        img = cv2.imread(str(path), cv2.IMREAD_COLOR)
        if img is None:
            return None

        h, w = img.shape[:2]
        scale = max(OUTPUT_WIDTH / w, OUTPUT_HEIGHT / h)
        resized = cv2.resize(img, (int(w * scale), int(h * scale)), interpolation=cv2.INTER_AREA)
        rh, rw = resized.shape[:2]
        x0 = max(0, (rw - OUTPUT_WIDTH) // 2)
        y0 = max(0, (rh - OUTPUT_HEIGHT) // 2)
        crop = resized[y0:y0 + OUTPUT_HEIGHT, x0:x0 + OUTPUT_WIDTH]
        crop = cv2.GaussianBlur(crop, (0, 0), 5)
        dark = np.zeros_like(crop)
        return cv2.addWeighted(crop, 1.0 - DARKEN_BACKGROUND, dark, DARKEN_BACKGROUND, 0)

    def pf(self, x: float, y: float) -> Tuple[int, int]:
        return int(round(self.origin_x + x * self.scale)), int(round(self.origin_y + y * self.scale))

    def frame_at_song_time(self, song_t: int) -> ReplayFrame:
        replay_t = song_t - self.replay_to_song_offset
        idx = bisect.bisect_left(self.times, replay_t)

        if idx <= 0:
            f = self.frames[0]
            return ReplayFrame(song_t, f.x, f.y, f.k1, f.k2, f.m1, f.m2)
        if idx >= len(self.frames):
            f = self.frames[-1]
            return ReplayFrame(song_t, f.x, f.y, f.k1, f.k2, f.m1, f.m2)

        a = self.frames[idx - 1]
        b = self.frames[idx]
        span = max(1, b.t - a.t)
        u = (replay_t - a.t) / span

        return ReplayFrame(
            song_t,
            a.x + (b.x - a.x) * u,
            a.y + (b.y - a.y) * u,
            a.k1 if u < 0.5 else b.k1,
            a.k2 if u < 0.5 else b.k2,
            a.m1 if u < 0.5 else b.m1,
            a.m2 if u < 0.5 else b.m2,
        )

    def draw_text(self, img, text, x, y, scale=0.6, thickness=2, color=COLOR_TEXT):
        cv2.putText(img, text, (x + 2, y + 2), cv2.FONT_HERSHEY_SIMPLEX, scale, COLOR_SHADOW, thickness + 2, cv2.LINE_AA)
        cv2.putText(img, text, (x, y), cv2.FONT_HERSHEY_SIMPLEX, scale, color, thickness, cv2.LINE_AA)

    def draw_filled_circle_alpha(self, img, center, radius, color, alpha):
        alpha = max(0.0, min(1.0, float(alpha)))
        if alpha <= 0.001 or radius <= 0:
            return
        if alpha >= 0.999:
            cv2.circle(img, center, radius, color, -1, cv2.LINE_AA)
            return

        # Optimized alpha blend: blend only the circle's bounding box, not the full frame.
        h, w = img.shape[:2]
        cx, cy = int(center[0]), int(center[1])
        pad = int(radius) + 4
        x0, y0 = max(0, cx - pad), max(0, cy - pad)
        x1, y1 = min(w, cx + pad + 1), min(h, cy + pad + 1)
        if x1 <= x0 or y1 <= y0:
            return

        roi = img[y0:y1, x0:x1]
        overlay = roi.copy()
        cv2.circle(overlay, (cx - x0, cy - y0), radius, color, -1, cv2.LINE_AA)
        cv2.addWeighted(overlay, alpha, roi, 1.0 - alpha, 0.0, roi)

    def draw_polyline_alpha(self, img, pts, color, thickness, alpha):
        alpha = max(0.0, min(1.0, float(alpha)))
        if alpha <= 0.001 or pts is None or len(pts) == 0:
            return
        if alpha >= 0.999:
            cv2.polylines(img, [pts], False, color, thickness, cv2.LINE_AA)
            return

        # Optimized alpha blend: blend only the polyline's bounding box, not the full frame.
        h, w = img.shape[:2]
        x, y, bw, bh = cv2.boundingRect(pts)
        pad = max(4, int(thickness) + 4)
        x0, y0 = max(0, x - pad), max(0, y - pad)
        x1, y1 = min(w, x + bw + pad), min(h, y + bh + pad)
        if x1 <= x0 or y1 <= y0:
            return

        roi = img[y0:y1, x0:x1]
        overlay = roi.copy()
        local_pts = pts.copy()
        local_pts[:, :, 0] -= x0
        local_pts[:, :, 1] -= y0
        cv2.polylines(overlay, [local_pts], False, color, thickness, cv2.LINE_AA)
        cv2.addWeighted(overlay, alpha, roi, 1.0 - alpha, 0.0, roi)

    def draw_circle_outline_alpha(self, img, center, radius, color, thickness, alpha):
        alpha = max(0.0, min(1.0, float(alpha)))
        if alpha <= 0.001 or radius <= 0:
            return
        if alpha >= 0.999:
            cv2.circle(img, center, radius, color, thickness, cv2.LINE_AA)
            return

        # Optimized alpha blend: blend only the circle outline's bounding box, not the full frame.
        h, w = img.shape[:2]
        cx, cy = int(center[0]), int(center[1])
        pad = int(radius) + int(thickness) + 4
        x0, y0 = max(0, cx - pad), max(0, cy - pad)
        x1, y1 = min(w, cx + pad + 1), min(h, cy + pad + 1)
        if x1 <= x0 or y1 <= y0:
            return

        roi = img[y0:y1, x0:x1]
        overlay = roi.copy()
        cv2.circle(overlay, (cx - x0, cy - y0), radius, color, thickness, cv2.LINE_AA)
        cv2.addWeighted(overlay, alpha, roi, 1.0 - alpha, 0.0, roi)

    def make_base_template(self) -> np.ndarray:
        img = self.background.copy() if self.background is not None else np.full((OUTPUT_HEIGHT, OUTPUT_WIDTH, 3), COLOR_BG, dtype=np.uint8)
        self.draw_field(img)
        return img

    def base_frame(self) -> np.ndarray:
        return self.base_template.copy()

    def draw_field(self, img):
        overlay = img.copy()
        cv2.rectangle(overlay, (self.origin_x, self.origin_y), (self.origin_x + self.field_w, self.origin_y + self.field_h), COLOR_FIELD, -1)
        cv2.addWeighted(overlay, 0.72, img, 0.28, 0, img)

        if DRAW_PLAYFIELD_BORDER:
            cv2.rectangle(img, (self.origin_x, self.origin_y), (self.origin_x + self.field_w, self.origin_y + self.field_h), COLOR_GRID, 2)

        if DRAW_PLAYFIELD_GRID:
            for x in range(0, 513, 128):
                px, _ = self.pf(x, 0)
                cv2.line(img, (px, self.origin_y), (px, self.origin_y + self.field_h), COLOR_GRID, 1)
            for y in range(0, 385, 96):
                _, py = self.pf(0, y)
                cv2.line(img, (self.origin_x, py), (self.origin_x + self.field_w, py), COLOR_GRID, 1)

    def object_color(self, obj: HitObject) -> Tuple[int, int, int]:
        judged = self.judgments.get(obj.index)
        if COLOR_OBJECTS_BY_RESULT and judged:
            return judgment_color(judged.result)
        return COMBO_COLORS[(obj.index - 1) % len(COMBO_COLORS)]

    def draw_slider_body(self, img, obj: HitObject, song_t: int, color):
        path = obj.slider_path or obj.control_points
        if not path or len(path) < 2:
            return

        visible_path = path

        # Snaking in: before hit time, reveal from head toward tail.
        if song_t < obj.t and ENABLE_SNAKING_IN:
            intro_u = clamp01(1.0 - ((obj.t - song_t) / float(max(1, SNAKE_IN_DURATION_MS))))
            visible_path = slice_path_by_fraction(path, 0.0, intro_u)

        # Snaking out: while active, only show the untraveled portion.
        elif obj.t <= song_t <= obj.end_t and ENABLE_SNAKING_OUT:
            _, path_u, reverse = slider_repeat_state(obj, song_t)
            if reverse:
                visible_path = slice_path_by_fraction(path, 0.0, path_u)
            else:
                visible_path = slice_path_by_fraction(path, path_u, 1.0)

        if len(visible_path) == 1:
            visible_path = [visible_path[0], visible_path[0]]

        pts = np.array([self.pf(x, y) for x, y in visible_path], dtype=np.int32).reshape((-1, 1, 2))
        body_thickness = max(8, self.circle_radius * 2)
        self.draw_polyline_alpha(img, pts, color, body_thickness, self.slider_fill_alpha)
        cv2.polylines(img, [pts], False, COLOR_OBJECT, max(2, self.circle_radius // 5), cv2.LINE_AA)

        sx, sy = self.pf(obj.x, obj.y)
        ex, ey = self.pf(*visible_path[-1])

        # Only draw the slider head circle before the slider starts.
        # Once active, the moving slider ball/follow circle communicates the active slider state.
        if song_t < obj.t:
            self.draw_filled_circle_alpha(img, (sx, sy), self.circle_radius, color, self.circle_fill_alpha)
            cv2.circle(img, (sx, sy), self.circle_radius, COLOR_OBJECT, 2, cv2.LINE_AA)

        # Tail / visible path end marker.
        cv2.circle(img, (ex, ey), self.circle_radius, color, 2, cv2.LINE_AA)
        cv2.circle(img, (ex, ey), max(4, self.circle_radius // 5), COLOR_OBJECT, -1, cv2.LINE_AA)

        if DRAW_SLIDER_TICKS and obj.slider_tick_times:
            for tick_t in obj.slider_tick_times:
                if tick_t < obj.t or tick_t > obj.end_t:
                    continue

                if song_t < obj.t and ENABLE_SNAKING_IN:
                    reveal_u = clamp01(1.0 - ((obj.t - song_t) / float(max(1, SNAKE_IN_DURATION_MS))))
                    tick_u = clamp01((tick_t - obj.t) / float(max(1, obj.duration)))
                    if tick_u > reveal_u:
                        continue
                elif obj.t <= song_t <= obj.end_t and ENABLE_SNAKING_OUT:
                    if tick_t < song_t:
                        continue

                tx, ty = self.pf(*slider_position(obj, tick_t))
                cv2.circle(img, (tx, ty), max(3, self.circle_radius // 8), COLOR_SLIDER_TICK, -1)

    def draw_slider_ball(self, img, obj: HitObject, song_t: int, color):
        if not DRAW_SLIDER_BALL or obj.kind != "slider":
            return
        if song_t < obj.t or song_t > obj.end_t:
            return

        bx, by = self.pf(*slider_position(obj, song_t))
        ball_r = max(6, int(self.circle_radius * SLIDER_BALL_RADIUS_SCALE))
        self.draw_filled_circle_alpha(img, (bx, by), ball_r, color, self.slider_ball_alpha)
        cv2.circle(img, (bx, by), ball_r, COLOR_OBJECT, 2, cv2.LINE_AA)

        if DRAW_SLIDER_FOLLOW_CIRCLE:
            follow_r = max(ball_r + 4, int(self.circle_radius * FOLLOW_CIRCLE_RADIUS_SCALE))
            self.draw_circle_outline_alpha(img, (bx, by), follow_r, COLOR_OBJECT, 2, self.follow_circle_alpha)

    def draw_object_judgment(self, img, obj: HitObject, song_t: int):
        if not DRAW_JUDGMENTS:
            return
        j = self.judgments.get(obj.index)
        if not j:
            return
        if j.result == "Great" and not JUDGMENT_SHOW_GREAT:
            return

        judgment_t = j.judgment_t if j.judgment_t is not None else (obj.end_t if obj.kind == "slider" else obj.t)
        age = song_t - judgment_t
        if age < 0 or age > JUDGMENT_TEXT_MS:
            return

        px, py = self.pf(obj.x, obj.y)
        main_color = judgment_color(j.result)
        label = judgment_label(j.result).strip()

        if j.result == "Miss" and JUDGMENT_DRAW_MISS_X:
            s = max(MISS_X_SIZE, self.circle_radius // 2)
            cv2.line(img, (px - s, py - s), (px + s, py + s), main_color, 3, cv2.LINE_AA)
            cv2.line(img, (px - s, py + s), (px + s, py - s), main_color, 3, cv2.LINE_AA)

        if label:
            scale = 0.64
            thickness = 2
            size = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, scale, thickness)[0]

            if JUDGMENT_TEXT_POSITION == "above":
                tx = px - size[0] // 2
                ty = py - self.circle_radius - 12
            elif JUDGMENT_TEXT_POSITION == "below":
                tx = px - size[0] // 2
                ty = py + self.circle_radius + size[1] + 12
            elif JUDGMENT_TEXT_POSITION == "left":
                tx = px - self.circle_radius - size[0] - 12
                ty = py + size[1] // 2
            elif JUDGMENT_TEXT_POSITION == "right":
                tx = px + self.circle_radius + 12
                ty = py + size[1] // 2
            else:
                tx = px - size[0] // 2
                ty = py + size[1] // 2

            tx += JUDGMENT_TEXT_OFFSET_X
            ty += JUDGMENT_TEXT_OFFSET_Y
            self.draw_text(img, label, tx, ty, scale=scale, thickness=thickness, color=main_color)

        # Optional debug detail for slider-follow status. Disabled by default so the popups stay osu-like.
        if JUDGMENT_SHOW_SLIDER_DETAILS and obj.kind == "slider" and j.head_hit:
            if j.slider_break:
                detail_parts = []
                if j.slider_ticks_total > 0:
                    detail_parts.append(f"{j.slider_ticks_hit}/{j.slider_ticks_total} ticks")
                if not j.tail_hit:
                    detail_parts.append("tail miss")

                detail = "Break"
                if detail_parts:
                    detail += " | " + ", ".join(detail_parts)

                self.draw_text(img, detail, px - 60, py - self.circle_radius - 40, scale=0.45, thickness=2, color=COLOR_MISS)
            elif j.slider_ticks_total > 0:
                detail = f"Held | {j.slider_ticks_hit}/{j.slider_ticks_total} ticks"
                self.draw_text(img, detail, px - 72, py - self.circle_radius - 40, scale=0.42, thickness=1, color=COLOR_GREAT)

    def draw_objects(self, img, song_t: int):
        if not DRAW_BEATMAP_OBJECTS or not self.objects:
            return

        start = bisect.bisect_left(self.object_times, song_t - 10000)
        end = bisect.bisect_right(self.object_times, song_t + self.preempt)

        for obj in self.objects[start:end]:
            if obj.kind == "spinner":
                continue

            j = self.judgments.get(obj.index)

            if song_t < obj.t - self.preempt:
                continue

            # Circles disappear the moment they are hit, or once they are considered missed.
            if obj.kind == "circle":
                if j and j.resolved_t is not None and song_t > j.resolved_t:
                    # Keep only the judgment text/X briefly after the circle vanishes.
                    self.draw_object_judgment(img, obj, song_t)
                    continue

            # Sliders stay visible while active, then disappear after they finish.
            elif obj.kind == "slider":
                if song_t > obj.end_t:
                    # Keep only the judgment text/X briefly after the slider vanishes.
                    self.draw_object_judgment(img, obj, song_t)
                    continue

            px, py = self.pf(obj.x, obj.y)
            color = self.object_color(obj)

            if obj.kind == "slider":
                self.draw_slider_body(img, obj, song_t, color)
                self.draw_slider_ball(img, obj, song_t, color)
            else:
                self.draw_filled_circle_alpha(img, (px, py), self.circle_radius, color, self.circle_fill_alpha)
                cv2.circle(img, (px, py), self.circle_radius, COLOR_OBJECT, 2, cv2.LINE_AA)

            if DRAW_APPROACH_CIRCLES and song_t <= obj.t:
                dt = obj.t - song_t
                u = max(0.0, min(1.0, dt / float(self.preempt)))
                approach_r = int(self.circle_radius * (1.0 + 3.2 * u))
                cv2.circle(img, (px, py), approach_r, COLOR_APPROACH, 2)

            if DRAW_OBJECT_NUMBERS:
                show_label = True
                if obj.kind == "slider" and song_t >= obj.t:
                    show_label = False
                if show_label:
                    size = cv2.getTextSize(obj.label, cv2.FONT_HERSHEY_SIMPLEX, 0.58, 2)[0]
                    self.draw_text(img, obj.label, px - size[0] // 2, py + size[1] // 2, scale=0.58, thickness=2)

            self.draw_object_judgment(img, obj, song_t)

    def draw_trail(self, img, song_t: int):
        if not DRAW_CURSOR_TRAIL:
            return

        replay_t = song_t - self.replay_to_song_offset
        idx0 = bisect.bisect_left(self.times, replay_t - TRAIL_MS)
        idx1 = bisect.bisect_right(self.times, replay_t)

        for i, f in enumerate(self.frames[idx0:idx1]):
            alpha = i / max(1, idx1 - idx0 - 1)
            radius = max(1, int(2 + 3 * alpha))
            px, py = self.pf(f.x, f.y)
            cv2.circle(img, (px, py), radius, (155, 155, 160), -1)

    def draw_cursor(self, img, f: ReplayFrame):
        px, py = self.pf(f.x, f.y)
        cv2.circle(img, (px, py), 11, COLOR_CURSOR, 2)
        cv2.circle(img, (px, py), 4, COLOR_CURSOR, -1)
        cv2.line(img, (px - 17, py), (px - 6, py), COLOR_CURSOR, 1)
        cv2.line(img, (px + 6, py), (px + 17, py), COLOR_CURSOR, 1)
        cv2.line(img, (px, py - 17), (px, py - 6), COLOR_CURSOR, 1)
        cv2.line(img, (px, py + 6), (px, py + 17), COLOR_CURSOR, 1)

    def draw_clicks(self, img, song_t: int):
        if not DRAW_CLICK_PULSES:
            return

        start = bisect.bisect_left(self.click_song_times, song_t - PULSE_DURATION_MS)
        end = bisect.bisect_right(self.click_song_times, song_t + 20)

        for i in range(start, end):
            c = self.clicks[i]
            c_song_t = c.t + self.replay_to_song_offset
            age = song_t - c_song_t
            if age < 0 or age > PULSE_DURATION_MS:
                continue

            u = age / float(PULSE_DURATION_MS)
            px, py = self.pf(c.x, c.y)
            color = COLOR_K1 if c.key == LABEL_K1 else COLOR_K2
            radius = int(18 + 56 * u)
            thickness = max(1, int(5 * (1 - u)) + 1)
            cv2.circle(img, (px, py), radius, color, thickness)

            if age <= FLASH_DURATION_MS:
                cv2.circle(img, (px, py), 8, color, -1)

            self.draw_text(img, c.key, px + 14, py - 14, scale=0.68, thickness=2, color=color)

    def draw_key_boxes(self, img, f: ReplayFrame):
        if not DRAW_KEY_BOXES:
            return

        x = 32
        y = OUTPUT_HEIGHT - 82
        w = 70
        h = 50
        gap = 12

        entries = [
            (LABEL_K1, f.k1 or f.m1, COLOR_K1),
            (LABEL_K2, f.k2 or f.m2, COLOR_K2),
        ]

        for i, (label, held, color) in enumerate(entries):
            x0 = x + i * (w + gap)
            cv2.rectangle(img, (x0, y), (x0 + w, y + h), COLOR_HELD if held else COLOR_UP, -1)
            cv2.rectangle(img, (x0, y), (x0 + w, y + h), color, 2)
            self.draw_text(img, label, x0 + 21, y + 34, scale=1.0, thickness=2)

    def draw_timeline(self, img, song_t: int):
        if not DRAW_TIMELINE:
            return

        x0 = 178
        x1 = OUTPUT_WIDTH - 32
        y0 = OUTPUT_HEIGHT - 66
        y1 = OUTPUT_HEIGHT - 22

        overlay = img.copy()
        cv2.rectangle(overlay, (x0, y0), (x1, y1), COLOR_PANEL, -1)
        cv2.addWeighted(overlay, 0.78, img, 0.22, 0, img)
        cv2.rectangle(img, (x0, y0), (x1, y1), COLOR_GRID, 1)

        self.draw_text(img, LABEL_K1, x0 + 8, y0 + 17, scale=0.42, thickness=1, color=COLOR_K1)
        self.draw_text(img, LABEL_K2, x0 + 8, y0 + 37, scale=0.42, thickness=1, color=COLOR_K2)

        left = x0 + 40
        right = x1 - 14
        cv2.line(img, (right, y0 + 5), (right, y1 - 5), COLOR_CURSOR, 2)

        start = bisect.bisect_left(self.click_song_times, song_t - TIMELINE_WINDOW_MS)
        end = bisect.bisect_right(self.click_song_times, song_t)

        for i in range(start, end):
            c = self.clicks[i]
            age = song_t - (c.t + self.replay_to_song_offset)
            x = right - int((age / float(TIMELINE_WINDOW_MS)) * (right - left))
            y = y0 + 14 if c.key == LABEL_K1 else y0 + 34
            cv2.circle(img, (x, y), 5, COLOR_K1 if c.key == LABEL_K1 else COLOR_K2, -1)

    def draw_header(self, img, song_t: int):
        if not DRAW_HEADER:
            return

        if self.beatmap:
            title = f"{self.beatmap.artist} - {self.beatmap.title} [{self.beatmap.version}]"
        else:
            title = "Beatmap not found - export matching .osz for circles/audio"

        self.draw_text(img, title, 28, 32, scale=0.62, thickness=2)
        audio_label = self.audio_path.name if self.audio_path else "no audio found"
        stats = (
            f"Replay: {getattr(self.replay, 'username', 'unknown')} | "
            f"Misses: {getattr(self.replay, 'count_miss', '?')} | "
            f"Song: {song_t / 1000:.2f}s | "
            f"Offset: {self.replay_to_song_offset}ms | "
            f"Audio: {audio_label}"
        )
        self.draw_text(img, stats, 28, 57, scale=0.42, thickness=1)

    def judgment_totals_at_time(self, song_t: int) -> Dict[str, int]:
        if self.judgment_totals_last_t is None or song_t < self.judgment_totals_last_t:
            self.judgment_totals = {"Great": 0, "Ok": 0, "Meh": 0, "Miss": 0}
            self.judgment_totals_idx = 0

        while self.judgment_totals_idx < len(self.judgment_events_sorted):
            event = self.judgment_events_sorted[self.judgment_totals_idx]
            resolved_t = int(event.resolved_t) if event.resolved_t is not None else 10**12
            if resolved_t > song_t:
                break
            self.judgment_totals[event.result] = self.judgment_totals.get(event.result, 0) + 1
            self.judgment_totals_idx += 1

        self.judgment_totals_last_t = song_t
        return dict(self.judgment_totals)

    def draw_judgment_totals_hud(self, img, song_t: int):
        if not DRAW_JUDGMENT_TOTALS:
            return

        totals = self.judgment_totals_at_time(song_t)
        label_great = JUDGMENT_TEXT_LABELS.get("Great", "Great")
        label_ok = JUDGMENT_TEXT_LABELS.get("Ok", "100")
        label_meh = JUDGMENT_TEXT_LABELS.get("Meh", "50")
        label_miss = JUDGMENT_TEXT_LABELS.get("Miss", "Miss")
        summary = (
            f"{label_great} {totals.get('Great', 0)} | "
            f"{label_ok} {totals.get('Ok', 0)} | "
            f"{label_meh} {totals.get('Meh', 0)} | "
            f"{label_miss} {totals.get('Miss', 0)}"
        )
        size, _ = cv2.getTextSize(summary, cv2.FONT_HERSHEY_SIMPLEX, 0.50, 1)
        x = OUTPUT_WIDTH - size[0] - 30
        y = 57 if DRAW_HEADER else 32
        self.draw_text(img, summary, x, y, scale=0.50, thickness=1)

    def render_frame(self, song_t: int) -> np.ndarray:
        f = self.frame_at_song_time(song_t)
        img = self.base_frame()
        self.draw_objects(img, song_t)
        self.draw_trail(img, song_t)
        self.draw_cursor(img, f)
        self.draw_clicks(img, song_t)
        self.draw_key_boxes(img, f)
        self.draw_timeline(img, song_t)
        self.draw_header(img, song_t)
        self.draw_judgment_totals_hud(img, song_t)
        return img

    def compute_render_range(self) -> Tuple[int, int, int]:
        if self.beatmap and self.beatmap.objects:
            first_object = self.beatmap.objects[0].t
            last_object = max(o.end_t if o.end_t else o.t for o in self.beatmap.objects)
        else:
            first_object = 0
            last_object = self.frames[-1].t + self.replay_to_song_offset

        if RENDER_START_MODE == "auto_first_object":
            start_t = max(0, first_object - self.preempt - 800)
        else:
            start_t = 0

        replay_end_t = self.frames[-1].t + self.replay_to_song_offset
        map_end_t = last_object
        if RENDER_END_MODE == "map_end":
            end_t = max(map_end_t, replay_end_t) + RENDER_END_PADDING_MS
        else:
            end_t = replay_end_t + RENDER_END_PADDING_MS

        total_frames = max(1, int(math.ceil((end_t - start_t) / 1000.0 * OUTPUT_FPS)))
        return start_t, end_t, total_frames

    def render_silent_range(self, output_path: str, start_t: int, end_t: int, progress_label: str = "") -> None:
        total_frames = max(1, int(math.ceil((end_t - start_t) / 1000.0 * OUTPUT_FPS)))
        writer = FFmpegVideoWriter(output_path, OUTPUT_WIDTH, OUTPUT_HEIGHT, OUTPUT_FPS)

        print()
        label = f" {progress_label}" if progress_label else ""
        print(f"Rendering replay visualization video{label}...")
        print(f"Silent temp: {os.path.abspath(output_path)}")
        print(f"Frames: {total_frames}")
        print(f"Resolution: {OUTPUT_WIDTH}x{OUTPUT_HEIGHT} @ {OUTPUT_FPS} FPS")
        print(f"Render encoder: {RENDER_ENCODER}")
        print(f"Quality profile: {QUALITY_PROFILE}")
        print(f"Performance mode: {PERFORMANCE_MODE}")
        print(f"Visual style: {self.visual_style}")
        print(f"Playfield origin: ({self.origin_x}, {self.origin_y}), scale: {self.scale:.3f}, circle radius: {self.circle_radius}px")
        if self.beatmap:
            sliders = sum(1 for o in self.objects if o.kind == "slider")
            print(f"Objects: {len(self.objects)} total, {sliders} sliders")

        last_print = time.perf_counter()
        for i in range(total_frames):
            song_t = int(round(start_t + (i / OUTPUT_FPS) * 1000.0))
            img = self.render_frame(song_t)
            writer.write(img)

            now = time.perf_counter()
            if now - last_print > RENDER_LOG_INTERVAL_SECONDS:
                print(f"  rendered {i + 1}/{total_frames} frames ({(i + 1) / total_frames * 100:.1f}%)", flush=True)
                last_print = now

        writer.release()

    def render_silent_parallel(self, start_t: int, end_t: int, workers: int) -> None:
        if imageio_ffmpeg is None:
            raise RuntimeError("imageio-ffmpeg is required for parallel rendering.")
        if self.replay_path is None:
            print("Parallel render disabled because replay path was not available.")
            self.render_silent_range(TEMP_VIDEO_PATH, start_t, end_t)
            return

        target_chunk_ms = max(1000, int(SMART_CHUNK_TARGET_SECONDS * 1000))
        render_duration_ms = max(1, int(end_t - start_t))

        def choose_balanced_chunk_count(duration_ms: int, target_ms: int, worker_count: int) -> int:
            """Choose a chunk count that stays close to the requested chunk size while avoiding a tiny final worker wave."""
            ideal = max(1, int(math.ceil(duration_ms / float(target_ms))))
            if worker_count <= 1:
                return ideal

            # Do not create chunks shorter than about 1 second just to satisfy alignment.
            max_chunks = max(1, int(math.ceil(duration_ms / 1000.0)))

            candidates = {ideal}
            lower = (ideal // worker_count) * worker_count
            upper = lower + worker_count
            for value in (lower, upper):
                if value > 0:
                    candidates.add(value)

            # If the render naturally needs fewer chunks than workers, fewer chunks are fine.
            # If it needs more chunks than workers, prefer exact worker waves: 8, 16, 24, etc.
            aligned_candidates = [
                value for value in candidates
                if 1 <= value <= max_chunks and (value <= worker_count or value % worker_count == 0)
            ]
            usable_candidates = aligned_candidates or [value for value in candidates if 1 <= value <= max_chunks]

            def score(value: int) -> Tuple[float, int]:
                avg_ms = duration_ms / float(value)
                distance_from_target = abs(avg_ms - target_ms) / float(target_ms)
                # Tie-break toward fewer chunks, because each chunk has startup/concat overhead.
                return distance_from_target, value

            return min(usable_candidates, key=score)

        chunk_count = choose_balanced_chunk_count(render_duration_ms, target_chunk_ms, workers)

        # Use a unique chunk folder every run. This avoids WinError 32 when a previous aborted
        # render left worker processes/log files open in the old chunk folder.
        chunk_dir = Path(OUTPUT_DIR) / f"{Path(OUTPUT_VIDEO_PATH).stem}_chunks_{os.getpid()}_{int(time.time())}"
        chunk_dir.mkdir(parents=True, exist_ok=True)

        ranges = []
        for idx in range(1, chunk_count + 1):
            s = int(round(start_t + ((idx - 1) / float(chunk_count)) * render_duration_ms))
            e = int(round(start_t + (idx / float(chunk_count)) * render_duration_ms))
            if e <= s:
                e = s + 1
            out_path = chunk_dir / f"chunk_{idx:04d}.mp4"
            log_path = chunk_dir / f"chunk_{idx:04d}.log"
            ranges.append((idx, s, e, out_path, log_path))

        actual_chunk_seconds = render_duration_ms / float(chunk_count) / 1000.0
        waves = int(math.ceil(chunk_count / float(max(1, workers))))

        print()
        print("Parallel chunk rendering enabled.")
        print(f"Workers: {workers}")
        print(f"Smart chunk target: about {SMART_CHUNK_TARGET_SECONDS}s, balanced to worker waves")
        print(f"Balanced chunks: {len(ranges)} x about {actual_chunk_seconds:.1f}s ({waves} worker wave{'s' if waves != 1 else ''})")
        print(f"Chunk dir: {chunk_dir.resolve()}")

        active = []
        pending = list(ranges)
        completed = 0

        total_render_frames = max(1, int(math.ceil((end_t - start_t) / 1000.0 * OUTPUT_FPS)))
        chunk_total_frames = {
            chunk_idx: max(1, int(math.ceil((e - s) / 1000.0 * OUTPUT_FPS)))
            for chunk_idx, s, e, _, _ in ranges
        }
        chunk_progress_frames = {chunk_idx: 0 for chunk_idx, _, _, _, _ in ranges}
        progress_pattern = re.compile(r"rendered\s+(\d+)/(\d+)\s+frames")
        parallel_started_at = time.perf_counter()
        last_parallel_progress_print = parallel_started_at

        def format_duration(seconds: float) -> str:
            seconds = max(0, int(round(seconds)))
            hours, rem = divmod(seconds, 3600)
            minutes, secs = divmod(rem, 60)
            if hours:
                return f"{hours}h {minutes:02d}m {secs:02d}s"
            return f"{minutes}m {secs:02d}s"

        def read_chunk_progress(log_path: Path) -> Optional[Tuple[int, int]]:
            try:
                with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                    tail = f.readlines()[-50:]
            except OSError:
                return None

            for line in reversed(tail):
                m = progress_pattern.search(line)
                if m:
                    return int(m.group(1)), int(m.group(2))
            return None

        def print_parallel_progress(force: bool = False) -> None:
            nonlocal last_parallel_progress_print

            now = time.perf_counter()
            if not force and now - last_parallel_progress_print < RENDER_LOG_INTERVAL_SECONDS:
                return

            for entry in active:
                chunk_idx, _, _, _, log_path = entry["item"]
                parsed = read_chunk_progress(log_path)
                if parsed is None:
                    continue

                done_frames, reported_total = parsed
                limit = max(1, min(chunk_total_frames.get(chunk_idx, reported_total), reported_total))
                chunk_progress_frames[chunk_idx] = max(0, min(done_frames, limit))

            done = min(total_render_frames, sum(chunk_progress_frames.values()))
            elapsed = max(0.001, now - parallel_started_at)
            render_fps = done / elapsed if done > 0 else 0.0
            eta = (total_render_frames - done) / render_fps if render_fps > 0 else None

            active_chunks = [entry["item"][0] for entry in active]
            active_text = ",".join(str(n) for n in active_chunks) if active_chunks else "none"
            eta_text = format_duration(eta) if eta is not None else "estimating"

            print(
                f"  overall progress: {done}/{total_render_frames} frames "
                f"({done / total_render_frames * 100:.1f}%) | "
                f"chunks {completed}/{len(ranges)} done | active {active_text} | "
                f"speed {render_fps:.1f} fps | elapsed {format_duration(elapsed)} | ETA {eta_text}",
                flush=True,
            )

            last_parallel_progress_print = now

        def cleanup_active_workers() -> None:
            for entry in active:
                proc = entry.get("proc")
                log_handle = entry.get("log_handle")
                try:
                    if proc and proc.poll() is None:
                        proc.terminate()
                except Exception:
                    pass
                try:
                    if log_handle:
                        log_handle.close()
                except Exception:
                    pass

        def start_chunk(item):
            chunk_idx, s, e, out_path, log_path = item
            cmd = [
                sys.executable,
                "-u",
                os.path.abspath(__file__),
                "--no-ui",
                "--render-chunk",
                "--chunk-start", str(s),
                "--chunk-end", str(e),
                "--chunk-out", str(out_path),
                "--replay-path", str(self.replay_path),
                "--replay-to-song-offset", str(self.replay_to_song_offset),
            ]
            if self.beatmap and self.beatmap.path:
                cmd.extend(["--beatmap-path", str(self.beatmap.path)])
            log_handle = open(log_path, "w", encoding="utf-8", errors="replace", buffering=1)
            proc = subprocess.Popen(cmd, stdout=log_handle, stderr=subprocess.STDOUT)
            return {"item": item, "proc": proc, "log_handle": log_handle}

        try:
            while pending or active:
                while pending and len(active) < workers:
                    active.append(start_chunk(pending.pop(0)))

                time.sleep(0.25)
                still_active = []
                for entry in active:
                    proc = entry["proc"]
                    ret = proc.poll()
                    if ret is None:
                        still_active.append(entry)
                        continue

                    try:
                        entry["log_handle"].close()
                    except Exception:
                        pass
                    chunk_idx, s, e, out_path, log_path = entry["item"]
                    if ret != 0:
                        print(f"Chunk {chunk_idx} failed. Log: {log_path}")
                        # On Windows a log can still be locked for a moment after the child exits.
                        # Retry briefly instead of failing with WinError 32 while reading the log.
                        for _ in range(10):
                            try:
                                with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                                    tail = f.readlines()[-40:]
                                print("".join(tail))
                                break
                            except OSError:
                                time.sleep(0.2)
                        raise RuntimeError(f"Parallel render chunk {chunk_idx} failed with exit code {ret}")

                    completed += 1
                    chunk_progress_frames[chunk_idx] = chunk_total_frames[chunk_idx]
                    print(f"  completed chunk {completed}/{len(ranges)}", flush=True)

                active = still_active
                print_parallel_progress(force=False)

            print_parallel_progress(force=True)
        except Exception:
            cleanup_active_workers()
            raise

        def ffconcat_quote_path(path: Path) -> str:
            # The concat demuxer has its own quoting rules. Use forward slashes
            # to avoid Windows backslash escaping, then escape any single quotes
            # that come from a user-chosen output folder.
            text = str(Path(path).resolve()).replace("\\", "/")
            return "'" + text.replace("'", r"'\''") + "'"

        concat_path = chunk_dir / "concat_list.txt"
        with open(concat_path, "w", encoding="utf-8") as f:
            for _, _, _, out_path, _ in ranges:
                f.write("file " + ffconcat_quote_path(out_path) + chr(10))

        ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
        cmd = [
            ffmpeg,
            "-y",
            "-hide_banner",
            "-loglevel", "warning",
            "-f", "concat",
            "-safe", "0",
            "-i", str(concat_path),
            "-c", "copy",
            TEMP_VIDEO_PATH,
        ]
        print("Concatenating chunks...")
        subprocess.run(cmd, check=True)
        print("Parallel render complete:", os.path.abspath(TEMP_VIDEO_PATH))

    def render_silent(self) -> Tuple[int, int]:
        Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
        if not self.frames:
            raise RuntimeError("Replay has no cursor frames to render.")

        start_t, end_t, total_frames = self.compute_render_range()
        workers = resolve_parallel_workers()
        print()
        print(f"Total frames: {total_frames}")
        print(f"Parallel workers requested/resolved: {PARALLEL_WORKERS_CONFIG} -> {workers}")

        if workers <= 1:
            self.render_silent_range(TEMP_VIDEO_PATH, start_t, end_t)
        else:
            self.render_silent_parallel(start_t, end_t, workers)

        return start_t, end_t

    def mux_audio(self, start_t: int) -> None:
        if imageio_ffmpeg is None:
            print("imageio-ffmpeg not installed; leaving silent video only.")
            os.replace(TEMP_VIDEO_PATH, OUTPUT_VIDEO_PATH)
            return

        if self.audio_path is None:
            print("Could not find beatmap audio; leaving silent video only.")
            os.replace(TEMP_VIDEO_PATH, OUTPUT_VIDEO_PATH)
            return

        ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
        cmd = [ffmpeg, "-y", "-hide_banner", "-loglevel", "warning", "-i", TEMP_VIDEO_PATH]

        if start_t > 0:
            cmd.extend(["-ss", f"{start_t / 1000.0:.3f}"])

        cmd.extend([
            "-i", str(self.audio_path),
            "-map", "0:v:0",
            "-map", "1:a:0",
            "-c:v", "copy",
            "-c:a", "aac",
            "-shortest",
            OUTPUT_VIDEO_PATH,
        ])

        print("Muxing audio:", self.audio_path)
        try:
            subprocess.run(cmd, check=True)
            try:
                os.remove(TEMP_VIDEO_PATH)
            except OSError:
                pass
        except subprocess.CalledProcessError as exc:
            print("Audio mux failed; leaving silent video. Error:", exc)
            os.replace(TEMP_VIDEO_PATH, OUTPUT_VIDEO_PATH)

    def nearest_click_info(self, obj: HitObject) -> Tuple[Optional[int], Optional[float], str]:
        if not self.clicks or not self.click_song_times:
            return None, None, ""

        left = bisect.bisect_left(self.click_song_times, obj.t - DATA_NEAREST_CLICK_WINDOW_MS)
        right = bisect.bisect_right(self.click_song_times, obj.t + DATA_NEAREST_CLICK_WINDOW_MS)
        best = None
        best_key = None

        for idx in range(left, right):
            c = self.clicks[idx]
            click_t = self.click_song_times[idx]
            dt = int(round(click_t - obj.t))
            dist = math.hypot(c.x - obj.x, c.y - obj.y)
            key = (abs(dt), dist)
            if best_key is None or key < best_key:
                best_key = key
                best = (dt, dist, c.key)

        if best is None:
            return None, None, ""
        return best

    def build_data_sheet(self):
        if not GENERATE_DATA_SHEET or not self.objects:
            return

        Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
        replay_end_t = self.frames[-1].t + self.replay_to_song_offset if self.frames else 0
        rows = []
        counts = {"Great": 0, "Ok": 0, "Meh": 0, "Miss": 0}
        slider_breaks = 0
        included = 0

        for obj in self.objects:
            if obj.kind not in ("circle", "slider"):
                continue
            j = self.judgments.get(obj.index)
            if not j:
                continue

            judgment_t = j.judgment_t if j.judgment_t is not None else obj.t
            if judgment_t > replay_end_t:
                continue

            included += 1
            counts[j.result] = counts.get(j.result, 0) + 1
            if obj.kind == "slider" and j.slider_break:
                slider_breaks += 1

            nearest_dt, nearest_dist, nearest_key = self.nearest_click_info(obj)
            main_error = j.timing_error_ms
            if j.result == "Miss" and main_error is None:
                miss_timing_note = "no nearby click"
                if nearest_dt is not None:
                    if nearest_dt < 0:
                        miss_timing_note = f"nearest click {abs(nearest_dt)}ms early"
                    elif nearest_dt > 0:
                        miss_timing_note = f"nearest click {nearest_dt}ms late"
                    else:
                        miss_timing_note = "nearest click on time but likely off-position"
            else:
                miss_timing_note = ""

            rows.append({
                "object_index": obj.index,
                "kind": obj.kind,
                "time_ms": obj.t,
                "time_seconds": round(obj.t / 1000.0, 3),
                "x": obj.x,
                "y": obj.y,
                "result": j.result,
                "on": "yes" if j.result != "Miss" else "no",
                "timing_error_ms": "" if main_error is None else main_error,
                "early_late": "" if main_error is None else ("early" if main_error < 0 else "late" if main_error > 0 else "on-time"),
                "nearest_click_dt_ms": "" if nearest_dt is None else nearest_dt,
                "nearest_click_early_late": "" if nearest_dt is None else ("early" if nearest_dt < 0 else "late" if nearest_dt > 0 else "on-time"),
                "nearest_click_distance_pf": "" if nearest_dist is None else round(nearest_dist, 2),
                "nearest_click_key": nearest_key,
                "slider_break": "yes" if (obj.kind == "slider" and j.slider_break) else "no",
                "slider_ticks_hit": j.slider_ticks_hit,
                "slider_ticks_total": j.slider_ticks_total,
                "tail_hit": "yes" if j.tail_hit else "no",
                "note": miss_timing_note,
            })

        if not rows:
            print("No judged objects before replay end; skipping data sheet.")
            return

        fieldnames = list(rows[0].keys())
        with open(DATA_CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        on_count = sum(1 for r in rows if r["on"] == "yes")
        off_count = sum(1 for r in rows if r["on"] == "no")
        accuracy_like = (on_count / max(1, len(rows))) * 100.0

        def esc(v):
            return html.escape(str(v))

        summary_rows = [
            ("Objects analyzed before fail/end", included),
            ("On / non-Miss", on_count),
            ("Off / Miss", off_count),
            ("Debug hit rate", f"{accuracy_like:.2f}%"),
            ("Great", counts.get("Great", 0)),
            ("Ok", counts.get("Ok", 0)),
            ("Meh", counts.get("Meh", 0)),
            ("Miss", counts.get("Miss", 0)),
            ("Slider breaks", slider_breaks),
        ]

        css = """
        body { font-family: Segoe UI, Arial, sans-serif; background:#111; color:#eee; margin:24px; }
        h1, h2 { margin-bottom: 8px; }
        table { border-collapse: collapse; width: 100%; margin: 14px 0 28px; font-size: 13px; }
        th, td { border: 1px solid #333; padding: 6px 8px; text-align: left; }
        th { background: #222; position: sticky; top: 0; }
        tr:nth-child(even) { background: #181818; }
        .Great { color:#75e075; font-weight:700; }
        .Ok { color:#70beff; font-weight:700; }
        .Meh { color:#ffd94a; font-weight:700; }
        .Miss { color:#ff6060; font-weight:700; }
        .note { color:#ffcc66; }
        .small { color:#aaa; font-size:12px; }
        """
        html_rows = []
        for r in rows:
            result_class = esc(r["result"])
            html_rows.append("<tr>" + "".join(
                f"<td class='{result_class if k == 'result' else 'note' if k == 'note' else ''}'>{esc(v)}</td>"
                for k, v in r.items()
            ) + "</tr>")

        with open(DATA_HTML_PATH, "w", encoding="utf-8") as f:
            f.write("<!doctype html><html><head><meta charset='utf-8'>")
            f.write("<title>osu! Replay Data Sheet</title>")
            f.write(f"<style>{css}</style></head><body>")
            f.write("<h1>osu! Replay Data Sheet</h1>")
            f.write("<p class='small'>Timing error: negative = early, positive = late. Nearest click data helps diagnose Misses where no valid hit was found.</p>")
            f.write("<h2>Summary</h2><table><tr><th>Metric</th><th>Value</th></tr>")
            for k, v in summary_rows:
                f.write(f"<tr><td>{esc(k)}</td><td>{esc(v)}</td></tr>")
            f.write("</table>")
            f.write("<h2>Per-object results</h2><table><tr>")
            for name in fieldnames:
                f.write(f"<th>{esc(name)}</th>")
            f.write("</tr>")
            f.write(chr(10).join(html_rows))
            f.write("</table></body></html>")

        print("Data sheet CSV saved:", os.path.abspath(DATA_CSV_PATH))
        print("Data sheet HTML saved:", os.path.abspath(DATA_HTML_PATH))

    def build_miss_sheet(self):
        if not GENERATE_MISS_SHEET or not self.objects:
            return

        miss_items = []
        # Only include misses that occur before the replay ends/fails.
        # Without this, the sheet can include objects after the player has already failed.
        replay_end_t = self.frames[-1].t + self.replay_to_song_offset if self.frames else 0
        for obj in self.objects:
            j = self.judgments.get(obj.index)
            if not j or j.result != "Miss":
                continue
            snapshot_t = j.judgment_t if j.judgment_t is not None else (obj.t + self.hit_window_50)
            if snapshot_t > replay_end_t:
                continue
            miss_items.append((obj, j, snapshot_t))

        if not miss_items:
            print("No main Miss judgments found; skipping miss sheet.")
            return

        Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
        if SAVE_INDIVIDUAL_MISS_FRAMES:
            Path(MISS_FRAME_DIR).mkdir(parents=True, exist_ok=True)

        tiles = []
        sheet_scale = max(1, MISS_SHEET_SCALE)
        caption_h = MISS_SHEET_CAPTION_HEIGHT * sheet_scale
        caption_x = 10 * sheet_scale
        for i, (obj, j, snapshot_t) in enumerate(miss_items, start=1):
            frame = self.render_frame(snapshot_t)

            if SAVE_INDIVIDUAL_MISS_FRAMES:
                out_path = os.path.join(MISS_FRAME_DIR, f"miss_{i:02d}_obj_{obj.index}_{obj.kind}_{snapshot_t}ms.png")
                cv2.imwrite(out_path, frame, [cv2.IMWRITE_PNG_COMPRESSION, 3])

            h, w = frame.shape[:2]
            thumb_w = min(MISS_SHEET_THUMB_WIDTH * sheet_scale, w)
            thumb_h = int(round(h * (thumb_w / float(w))))
            if thumb_w == w and thumb_h == h:
                thumb = frame.copy()
            else:
                thumb = cv2.resize(frame, (thumb_w, thumb_h), interpolation=cv2.INTER_AREA)

            tile = np.full((thumb_h + caption_h, thumb_w, 3), COLOR_BG, dtype=np.uint8)
            tile[:thumb_h, :] = thumb

            line1 = f"Miss {i} | Obj {obj.index} | {obj.kind}"
            line2 = f"{snapshot_t / 1000.0:.3f}s"
            if j.timing_error_ms is not None:
                line2 += f" | err {j.timing_error_ms:+d}ms"

            cv2.putText(tile, line1, (caption_x, thumb_h + 20 * sheet_scale), cv2.FONT_HERSHEY_SIMPLEX, 0.55 * sheet_scale, COLOR_TEXT, max(1, sheet_scale), cv2.LINE_AA)
            cv2.putText(tile, line2, (caption_x, thumb_h + 44 * sheet_scale), cv2.FONT_HERSHEY_SIMPLEX, 0.50 * sheet_scale, COLOR_TEXT, max(1, sheet_scale), cv2.LINE_AA)
            tiles.append(tile)

        cols = max(1, MISS_SHEET_COLUMNS)
        rows = int(math.ceil(len(tiles) / float(cols)))
        tile_h = max(t.shape[0] for t in tiles)
        tile_w = max(t.shape[1] for t in tiles)
        pad = MISS_SHEET_PADDING * sheet_scale
        title_h = 70 * sheet_scale

        sheet_h = title_h + pad + rows * (tile_h + pad)
        sheet_w = pad + cols * (tile_w + pad)
        sheet = np.full((sheet_h, sheet_w, 3), COLOR_BG, dtype=np.uint8)

        cv2.putText(sheet, f"Miss Snapshot Sheet - {len(tiles)} miss events", (pad, 34 * sheet_scale), cv2.FONT_HERSHEY_SIMPLEX, 0.95 * sheet_scale, COLOR_TEXT, 2 * sheet_scale, cv2.LINE_AA)
        cv2.putText(sheet, "Each tile shows the exact main miss moment.", (pad, 60 * sheet_scale), cv2.FONT_HERSHEY_SIMPLEX, 0.55 * sheet_scale, COLOR_TEXT, max(1, sheet_scale), cv2.LINE_AA)

        for idx, tile in enumerate(tiles):
            r = idx // cols
            c = idx % cols
            x = pad + c * (tile_w + pad)
            y = title_h + pad + r * (tile_h + pad)
            th, tw = tile.shape[:2]
            sheet[y:y + th, x:x + tw] = tile

        jpg_params = [cv2.IMWRITE_JPEG_QUALITY, MISS_SHEET_JPEG_QUALITY]
        if hasattr(cv2, "IMWRITE_JPEG_OPTIMIZE"):
            jpg_params.extend([cv2.IMWRITE_JPEG_OPTIMIZE, 1])
        cv2.imwrite(MISS_SHEET_PATH, sheet, jpg_params)
        print("Miss sheet saved:", os.path.abspath(MISS_SHEET_PATH))
        print(f"Miss sheet quality: {sheet.shape[1]}x{sheet.shape[0]} px, JPEG quality {MISS_SHEET_JPEG_QUALITY}")

    def render(self):
        start_t, _ = self.render_silent()
        self.mux_audio(start_t)
        self.build_data_sheet()
        self.build_miss_sheet()
        print("Done.")
        print(os.path.abspath(OUTPUT_VIDEO_PATH))


# ============================================================
# MAIN
# ============================================================


def main():
    print(SCRIPT_VERSION)
    print("=" * len(SCRIPT_VERSION))

    osu_folder = find_osu_folder()
    replay_path = find_replay(osu_folder)

    print("osu folder:", osu_folder)
    print("Chosen replay:", replay_path)

    replay = Replay.from_path(replay_path)
    print("Replay user:", getattr(replay, "username", "unknown"))
    print("Replay timestamp:", replay_timestamp_value(replay, replay_path.stat().st_mtime)[1])
    print("Beatmap hash:", replay.beatmap_hash)
    print("Hit counts:", replay.count_300, replay.count_100, replay.count_50, "miss", replay.count_miss)

    frames, clicks = parse_replay_frames(replay)
    print(f"Parsed {len(frames)} replay frames and {len(clicks)} tap events.")

    beatmap = None
    beatmap_path = find_beatmap(osu_folder, replay.beatmap_hash, replay_path)
    while beatmap_path is None and GUIDE_MISSING_OSZ_EXPORT:
        exported_osz = wait_for_user_exported_osz(replay_path)
        if exported_osz is None:
            break
        print("Retrying beatmap match after new .osz export...")
        beatmap_path = find_beatmap(osu_folder, replay.beatmap_hash, replay_path)
        if beatmap_path is None:
            print("That .osz did not match this replay. Please export the exact beatmap/difficulty and the tool will keep waiting.")

    if beatmap_path:
        print("Beatmap:", beatmap_path)
        beatmap = parse_beatmap(beatmap_path)
        print(f"Parsed {len(beatmap.objects)} hit objects. AR={beatmap.ar}, CS={beatmap.cs}, OD={beatmap.od}")
        print(f"SliderMultiplier={beatmap.slider_multiplier}, SliderTickRate={beatmap.slider_tick_rate}, TimingPoints={len(beatmap.timing_points)}")
        print("Audio from .osu:", beatmap.audio_filename or "not listed")
        print("Resolved audio:", find_audio_file(beatmap) or "not found")
        print("Background:", beatmap.background_filename or "not found in .osu")
    else:
        print("Could not find matching .osu beatmap.")
        print("Keep the exported .osr and matching .osz in:")
        print("  C:/Users/Trave/AppData/Roaming/osu/exports")

    configure_output_paths_for_render(replay, replay_path, beatmap)

    offset = estimate_replay_to_song_offset(clicks, beatmap.objects if beatmap else [])
    Renderer(frames, clicks, beatmap, replay, offset, replay_path=replay_path).render()


def safe_path_component(text: str, max_len: int = 120) -> str:
    """Return a Windows-safe filename/folder component.

    Keep the name human-readable, but remove characters that commonly break
    Windows paths, FFmpeg concat files, or command-line tools. In particular,
    apostrophes are removed because FFmpeg concat uses single-quoted paths.
    """
    text = str(text or "").strip()
    text = re.sub(r"[<>:\"/\\|?*]+", " - ", text)
    text = re.sub(r"['`´‘’“”]+", "", text)
    text = re.sub(r"\s+", " ", text).strip(" .-_")
    if not text:
        text = "osu_replay_render"
    reserved = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}
    if text.upper() in reserved:
        text = "_" + text
    if len(text) > max_len:
        text = text[:max_len].rstrip(" .-_")
    return text or "osu_replay_render"


def replay_timestamp_for_path(replay: Replay, replay_path: Path) -> str:
    try:
        _, text_value = replay_timestamp_value(replay, replay_path.stat().st_mtime)
    except Exception:
        text_value = datetime.now().isoformat(timespec="seconds")
    text_value = text_value.replace("T", "_").replace(":", "-")
    text_value = re.sub(r"\s+file-mtime$", "", text_value)
    return safe_path_component(text_value, 40)


def build_render_basename(replay: Replay, replay_path: Path, beatmap: Optional[BeatmapInfo]) -> str:
    if beatmap:
        map_name = f"{beatmap.artist} - {beatmap.title} [{beatmap.version}]"
    else:
        map_name = replay_path.stem

    username = str(getattr(replay, "username", "unknown") or "unknown")
    timestamp = replay_timestamp_for_path(replay, replay_path)
    return safe_path_component(f"{map_name} - {username} - {timestamp}", 90)


def configure_output_paths_for_render(replay: Replay, replay_path: Path, beatmap: Optional[BeatmapInfo]) -> None:
    """Create a render-specific output folder and set all output filenames for this run."""
    global OUTPUT_DIR, OUTPUT_VIDEO_PATH, TEMP_VIDEO_PATH, EXTRACTED_OSZ_DIR
    global MISS_FRAME_DIR, MISS_SHEET_PATH, DATA_CSV_PATH, DATA_HTML_PATH

    base_output = Path(BASE_OUTPUT_DIR).expanduser()
    basename = build_render_basename(replay, replay_path, beatmap)
    render_dir = base_output / basename

    # Avoid silently overwriting a previous render of the same replay/map.
    if render_dir.exists():
        suffix = 2
        while True:
            candidate = base_output / f"{basename} ({suffix})"
            if not candidate.exists():
                render_dir = candidate
                break
            suffix += 1

    OUTPUT_DIR = str(render_dir)
    OUTPUT_VIDEO_PATH = str(render_dir / f"{basename}.mp4")
    TEMP_VIDEO_PATH = str(render_dir / f"{basename}_silent.mp4")
    EXTRACTED_OSZ_DIR = str(render_dir / "extracted_osz")
    MISS_FRAME_DIR = str(render_dir / f"{basename}_miss_frames")
    MISS_SHEET_PATH = str(render_dir / f"{basename}_miss_sheet.jpg")
    DATA_CSV_PATH = str(render_dir / f"{basename}_data_sheet.csv")
    DATA_HTML_PATH = str(render_dir / f"{basename}_data_sheet.html")

    render_dir.mkdir(parents=True, exist_ok=True)
    print("Output folder:", os.path.abspath(OUTPUT_DIR))
    print("Output video:", os.path.abspath(OUTPUT_VIDEO_PATH))


def save_config_file(cfg: Dict) -> None:
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)


def start_ui() -> None:
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox, ttk
    except Exception as exc:
        print("Tkinter UI unavailable, running in console mode:", exc)
        run_console_main()
        return

    NL = chr(10)
    root = tk.Tk()
    root.title("osu! Replay Click Visualizer")
    root.geometry("1340x1080")

    cfg = dict(CONFIG)

    visual_style_var = tk.StringVar(value=str(cfg.get("visual_style", "ghost")))
    encoder_var = tk.StringVar(value=str(cfg.get("render_encoder", "h264_nvenc")))
    quality_choices = ["fast", "balanced", "high", "max"]
    quality_var = tk.StringVar(value=str(cfg.get("quality_profile", "fast")).strip().lower())
    if quality_var.get() not in quality_choices:
        quality_var.set("fast")
    performance_var = tk.StringVar(value=str(cfg.get("performance_mode", "quality")))
    parallel_var = tk.StringVar(value=("Auto" if int(cfg.get("parallel_workers", 0) or 0) == 0 else str(int(cfg.get("parallel_workers", 0) or 0))))
    output_dir_var = tk.StringVar(value=str(cfg.get("output_dir", "") or BASE_OUTPUT_DIR or "osu_visualizer_output"))
    # Default the UI to the detected monitor resolution/refresh rate.
    # The config may store 0 for auto, but users should see/select the actual value they will get.
    detected_fps = OUTPUT_FPS
    detected_width = OUTPUT_WIDTH
    detected_height = OUTPUT_HEIGHT

    hz_options = [20, 24, 25, 30, 48, 50, 60, 72, 75, 90, 100, 120, 144, 155, 160, 165, 170, 175, 180, 200, 240, 280, 300, 360, 390, 480, 500, 540]
    if detected_fps not in hz_options:
        hz_options.append(detected_fps)
    hz_options = sorted(set(hz_options))
    fps_choices = [f"Auto ({detected_fps} Hz)"] + [f"{hz} Hz" for hz in hz_options]

    resolution_options = [
        (640, 480), (800, 600), (1024, 768), (1152, 864), (1280, 720), (1280, 800),
        (1280, 960), (1280, 1024), (1360, 768), (1366, 768), (1440, 900), (1600, 900),
        (1600, 1200), (1680, 1050), (1920, 1080), (1920, 1200), (2048, 1080),
        (2560, 1080), (2560, 1440), (2560, 1600), (3440, 1440), (3840, 1600),
        (3840, 2160), (4096, 2160), (5120, 1440), (5120, 2160), (5120, 2880),
        (6016, 3384), (7680, 4320)
    ]
    detected_resolution = (detected_width, detected_height)
    if detected_resolution not in resolution_options:
        resolution_options.append(detected_resolution)
    resolution_options = sorted(set(resolution_options), key=lambda wh: (wh[0] * wh[1], wh[0], wh[1]))
    resolution_choices = [f"Auto ({detected_width}x{detected_height})"] + [f"{w}x{h}" for w, h in resolution_options]

    configured_fps = int(cfg.get("render_fps", 0) or 0)
    configured_width = int(cfg.get("render_width", 0) or 0)
    configured_height = int(cfg.get("render_height", 0) or 0)
    fps_var = tk.StringVar(value=(f"{configured_fps} Hz" if configured_fps else f"Auto ({detected_fps} Hz)"))
    resolution_var = tk.StringVar(value=(f"{configured_width}x{configured_height}" if configured_width and configured_height else f"Auto ({detected_width}x{detected_height})"))
    watch_var = tk.BooleanVar(value=False)
    install_type_value = normalize_osu_install_type(cfg.get("osu_install_type", OSU_INSTALL_TYPE))
    install_type_var = tk.StringVar(value=("osu!" if install_type_value == "stable" else "osu!(lazer)"))
    exports_var = tk.StringVar(value=str(cfg.get("exports_dir", "") or REPLAY_FOLDER or ""))
    osu_root_var = tk.StringVar(value=str(cfg.get("osu_root_dir", "") or OSU_FOLDER or ""))
    replay_var = tk.StringVar(value=str(cfg.get("replay_path", "")))
    beatmap_var = tk.StringVar(value=str(cfg.get("beatmap_path", "")))
    miss_sheet_var = tk.BooleanVar(value=bool(cfg.get("generate_miss_sheet", True)))
    data_sheet_var = tk.BooleanVar(value=bool(cfg.get("generate_data_sheet", True)))
    snake_var = tk.StringVar(value=str(cfg.get("snake_in_duration_ms", 450)))

    custom_visual_options = [
        ("Background", "custom_draw_background"),
        ("Playfield border", "custom_draw_playfield_border"),
        ("Approach circles", "custom_draw_approach_circles"),
        ("Object numbers", "custom_draw_object_numbers"),
        ("Cursor trail", "custom_draw_cursor_trail"),
        ("Click pulses", "custom_draw_click_pulses"),
        ("Timeline", "custom_draw_timeline"),
        ("Key boxes", "custom_draw_key_boxes"),
        ("Header text", "custom_draw_header"),
        ("Slider tick dots", "custom_draw_slider_ticks"),
        ("Slider follow circle", "custom_draw_slider_follow_circle"),
        ("Judgment popups", "custom_draw_judgments"),
        ("Judgment totals HUD", "custom_draw_judgment_totals"),
    ]
    custom_visual_vars = {
        key: tk.BooleanVar(value=bool(cfg.get(key, DEFAULT_CONFIG.get(key, True))))
        for _, key in custom_visual_options
    }

    judgment_show_great_var = tk.BooleanVar(value=bool(cfg.get("judgment_show_great", False)))
    judgment_text_great_var = tk.StringVar(value=str(cfg.get("judgment_text_great", "Great")))
    judgment_text_ok_var = tk.StringVar(value=str(cfg.get("judgment_text_ok", "100")))
    judgment_text_meh_var = tk.StringVar(value=str(cfg.get("judgment_text_meh", "50")))
    judgment_text_miss_var = tk.StringVar(value=str(cfg.get("judgment_text_miss", "Miss")))
    judgment_duration_var = tk.StringVar(value=str(cfg.get("judgment_text_duration_ms", 300)))
    judgment_position_var = tk.StringVar(value=str(cfg.get("judgment_text_position", "center")))
    judgment_offset_x_var = tk.StringVar(value=str(cfg.get("judgment_text_offset_x", 0)))
    judgment_offset_y_var = tk.StringVar(value=str(cfg.get("judgment_text_offset_y", 0)))
    judgment_draw_miss_x_var = tk.BooleanVar(value=bool(cfg.get("judgment_draw_miss_x", False)))
    judgment_show_slider_details_var = tk.BooleanVar(value=bool(cfg.get("judgment_show_slider_details", False)))

    outer_container = tk.Frame(root)
    outer_container.pack(fill="both", expand=True)

    ui_canvas = tk.Canvas(outer_container, highlightthickness=0)
    ui_scrollbar = ttk.Scrollbar(outer_container, orient="vertical", command=ui_canvas.yview)
    ui_canvas.configure(yscrollcommand=ui_scrollbar.set)
    ui_scrollbar.pack(side="right", fill="y")
    ui_canvas.pack(side="left", fill="both", expand=True)

    main_frame = tk.Frame(ui_canvas, padx=14, pady=12)
    main_frame_window = ui_canvas.create_window((0, 0), window=main_frame, anchor="nw")

    def _refresh_ui_scrollregion(event=None):
        try:
            ui_canvas.configure(scrollregion=ui_canvas.bbox("all"))
        except Exception:
            pass

    def _resize_main_frame_to_canvas(event):
        try:
            ui_canvas.itemconfigure(main_frame_window, width=event.width)
        except Exception:
            pass
        _refresh_ui_scrollregion()

    def _on_ui_mousewheel(event):
        try:
            delta = event.delta
            if delta == 0:
                return
            ui_canvas.yview_scroll(int(-1 * (delta / 120)), "units")
        except Exception:
            pass

    def _on_ui_mousewheel_linux_up(event):
        ui_canvas.yview_scroll(-1, "units")

    def _on_ui_mousewheel_linux_down(event):
        ui_canvas.yview_scroll(1, "units")

    main_frame.bind("<Configure>", _refresh_ui_scrollregion)
    ui_canvas.bind("<Configure>", _resize_main_frame_to_canvas)
    ui_canvas.bind_all("<MouseWheel>", _on_ui_mousewheel)
    ui_canvas.bind_all("<Button-4>", _on_ui_mousewheel_linux_up)
    ui_canvas.bind_all("<Button-5>", _on_ui_mousewheel_linux_down)

    title = tk.Label(main_frame, text="osu! Replay Click Visualizer", font=("Segoe UI", 16, "bold"))
    title.grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 10))
    install_type_frame = tk.Frame(main_frame)
    install_type_frame.grid(row=0, column=2, columnspan=2, sticky="e", pady=(0, 10))
    tk.Label(install_type_frame, text="Type", anchor="e").pack(side="left", padx=(0, 6))
    install_type_combo = ttk.Combobox(install_type_frame, textvariable=install_type_var, values=["osu!(lazer)", "osu!"], state="readonly", width=13)
    install_type_combo.pack(side="left")

    def row_label(row: int, text: str):
        tk.Label(main_frame, text=text, anchor="w").grid(row=row, column=0, sticky="w", pady=4)

    def browse_dir(var: tk.StringVar):
        path = filedialog.askdirectory()
        if path:
            var.set(path)

    def browse_file(var: tk.StringVar, filetypes):
        path = filedialog.askopenfilename(filetypes=filetypes)
        if path:
            var.set(path)

    def default_paths_for_ui_install_type(display_value: str) -> Tuple[str, str]:
        kind = "stable" if str(display_value).strip().lower() in ("osu", "osu!") else "lazer"
        root, replay_folder = auto_detect_osu_paths(kind)
        if root and replay_folder:
            return root, replay_folder
        home = Path.home()
        appdata = os.environ.get("APPDATA")
        local = os.environ.get("LOCALAPPDATA")
        if kind == "stable":
            root_path = Path(local) / "osu!" if local else home / "AppData" / "Local" / "osu!"
            return str(root_path), str(root_path / "Replays")
        root_path = Path(appdata) / "osu" if appdata else home / "AppData" / "Roaming" / "osu"
        return str(root_path), str(root_path / "exports")

    def apply_install_type_defaults(*_):
        root_path, replay_folder = default_paths_for_ui_install_type(install_type_var.get())
        osu_root_var.set(root_path)
        exports_var.set(replay_folder)

    install_type_var.trace_add("write", apply_install_type_defaults)

    row_label(1, "Performance mode")
    ttk.Combobox(main_frame, textvariable=performance_var, values=["quality", "fast", "turbo", "custom"], state="readonly").grid(row=1, column=1, sticky="ew", pady=4)
    tk.Label(main_frame, text="Custom uses the visual-layer checkboxes below", anchor="w").grid(row=1, column=2, columnspan=2, sticky="w", pady=4)

    visual_preview_area = tk.Frame(main_frame)
    visual_preview_area.grid(row=2, column=0, columnspan=4, sticky="nsew", pady=(4, 8))
    visual_preview_area.grid_columnconfigure(0, weight=1, uniform="visual_halves")
    visual_preview_area.grid_columnconfigure(1, weight=1, uniform="visual_halves")
    visual_preview_area.grid_rowconfigure(0, weight=1)

    visual_options_frame = tk.Frame(visual_preview_area)
    visual_options_frame.grid(row=0, column=0, sticky="nsew")
    visual_options_frame.grid_columnconfigure(0, weight=1)

    custom_frame = ttk.LabelFrame(visual_options_frame, text="Custom visual layers (used only when Performance mode = custom)")
    custom_frame.grid(row=0, column=0, sticky="ew", pady=(0, 6), padx=(0, 6))
    for idx, (label, key) in enumerate(custom_visual_options):
        tk.Checkbutton(custom_frame, text=label, variable=custom_visual_vars[key]).grid(row=idx // 2, column=idx % 2, sticky="w", padx=8, pady=2)

    judgment_frame = ttk.LabelFrame(visual_options_frame, text="Judgment popup text")
    judgment_frame.grid(row=1, column=0, sticky="ew", padx=(0, 6))
    judgment_frame.grid_columnconfigure(0, weight=1, uniform="judgment_cols")
    judgment_frame.grid_columnconfigure(1, weight=1, uniform="judgment_cols")

    left_judgment_col = tk.Frame(judgment_frame)
    right_judgment_col = tk.Frame(judgment_frame)
    left_judgment_col.grid(row=0, column=0, sticky="nsew", padx=(8, 12), pady=6)
    right_judgment_col.grid(row=0, column=1, sticky="nsew", padx=(12, 8), pady=6)

    for col_frame in (left_judgment_col, right_judgment_col):
        col_frame.grid_columnconfigure(0, weight=0)
        col_frame.grid_columnconfigure(1, weight=1)

    tk.Checkbutton(left_judgment_col, text="Show Great", variable=judgment_show_great_var).grid(row=0, column=0, columnspan=2, sticky="w", pady=2)

    tk.Label(left_judgment_col, text="Great", anchor="w").grid(row=1, column=0, sticky="w", padx=(0, 6), pady=2)
    tk.Entry(left_judgment_col, textvariable=judgment_text_great_var, width=14).grid(row=1, column=1, sticky="ew", pady=2)
    tk.Label(left_judgment_col, text="Ok", anchor="w").grid(row=2, column=0, sticky="w", padx=(0, 6), pady=2)
    tk.Entry(left_judgment_col, textvariable=judgment_text_ok_var, width=14).grid(row=2, column=1, sticky="ew", pady=2)
    tk.Label(left_judgment_col, text="Meh", anchor="w").grid(row=3, column=0, sticky="w", padx=(0, 6), pady=2)
    tk.Entry(left_judgment_col, textvariable=judgment_text_meh_var, width=14).grid(row=3, column=1, sticky="ew", pady=2)
    tk.Label(left_judgment_col, text="Miss", anchor="w").grid(row=4, column=0, sticky="w", padx=(0, 6), pady=2)
    tk.Entry(left_judgment_col, textvariable=judgment_text_miss_var, width=14).grid(row=4, column=1, sticky="ew", pady=2)
    tk.Label(left_judgment_col, text="Duration ms", anchor="w").grid(row=5, column=0, sticky="w", padx=(0, 6), pady=2)
    tk.Entry(left_judgment_col, textvariable=judgment_duration_var, width=14).grid(row=5, column=1, sticky="ew", pady=2)

    tk.Label(right_judgment_col, text="Position", anchor="e").grid(row=0, column=0, sticky="e", padx=(0, 6), pady=2)
    ttk.Combobox(right_judgment_col, textvariable=judgment_position_var, values=["center", "above", "below", "left", "right"], state="readonly", width=12).grid(row=0, column=1, sticky="ew", pady=2)
    tk.Label(right_judgment_col, text="Offset X", anchor="e").grid(row=1, column=0, sticky="e", padx=(0, 6), pady=2)
    tk.Entry(right_judgment_col, textvariable=judgment_offset_x_var, width=14).grid(row=1, column=1, sticky="ew", pady=2)
    tk.Label(right_judgment_col, text="Offset Y", anchor="e").grid(row=2, column=0, sticky="e", padx=(0, 6), pady=2)
    tk.Entry(right_judgment_col, textvariable=judgment_offset_y_var, width=14).grid(row=2, column=1, sticky="ew", pady=2)
    tk.Checkbutton(right_judgment_col, text="Miss X", variable=judgment_draw_miss_x_var).grid(row=3, column=0, columnspan=2, sticky="w", pady=2)
    tk.Checkbutton(right_judgment_col, text="Slider details", variable=judgment_show_slider_details_var).grid(row=4, column=0, columnspan=2, sticky="w", pady=2)

    preview_frame = ttk.LabelFrame(visual_preview_area, text="Visual preview")
    preview_frame.grid(row=0, column=1, rowspan=2, sticky="nsew", padx=(6, 0))
    preview_frame.grid_columnconfigure(0, weight=1)
    preview_frame.grid_columnconfigure(1, weight=0)
    PREVIEW_W, PREVIEW_H = 540, 304
    preview_canvas = tk.Canvas(preview_frame, width=PREVIEW_W, height=PREVIEW_H, bg="#08080c", highlightthickness=1, highlightbackground="#333333")
    preview_canvas.grid(row=0, column=0, columnspan=2, sticky="n", padx=8, pady=(8, 4))
    preview_note = tk.Label(
        preview_frame,
        text="Approximate preview of the selected visual layers.",
        justify="left",
        wraplength=390,
        anchor="w",
    )
    preview_note.grid(row=1, column=0, sticky="w", padx=(8, 4), pady=(0, 8))

    def preview_layer_enabled(key: str) -> bool:
        mode = performance_var.get().strip().lower()
        if mode == "custom":
            var = custom_visual_vars.get(key)
            return bool(var.get()) if var is not None else True
        if mode == "fast":
            return key not in {"custom_draw_cursor_trail", "custom_draw_object_numbers", "custom_draw_slider_ticks", "custom_draw_timeline"}
        if mode == "turbo":
            return key not in {
                "custom_draw_background",
                "custom_draw_playfield_border",
                "custom_draw_approach_circles",
                "custom_draw_object_numbers",
                "custom_draw_cursor_trail",
                "custom_draw_timeline",
                "custom_draw_header",
                "custom_draw_slider_ticks",
                "custom_draw_slider_follow_circle",
                "custom_draw_judgment_totals",
            }
        return True

    def preview_judgment_label() -> str:
        label = judgment_text_ok_var.get().strip() or "100"
        if judgment_show_great_var.get():
            label = judgment_text_great_var.get().strip() or "Great"
        return label

    def preview_text_position(cx: int, cy: int, radius: int, text_width_guess: int) -> Tuple[int, int, str]:
        pos = judgment_position_var.get().strip().lower()
        try:
            ox = int(judgment_offset_x_var.get())
        except ValueError:
            ox = 0
        try:
            oy = int(judgment_offset_y_var.get())
        except ValueError:
            oy = 0

        if pos == "above":
            x, y, anchor_name = cx, cy - radius - 18, "s"
        elif pos == "below":
            x, y, anchor_name = cx, cy + radius + 18, "n"
        elif pos == "left":
            x, y, anchor_name = cx - radius - 18, cy, "e"
        elif pos == "right":
            x, y, anchor_name = cx + radius + 18, cy, "w"
        else:
            x, y, anchor_name = cx, cy, "center"
        return x + ox, y + oy, anchor_name

    def update_preview(*_args):
        c = preview_canvas
        c.delete("all")
        w, h = PREVIEW_W, PREVIEW_H
        aa_scale = 3
        W, H = w * aa_scale, h * aa_scale

        bg_enabled = preview_layer_enabled("custom_draw_background")
        border_enabled = preview_layer_enabled("custom_draw_playfield_border")
        approach_enabled = preview_layer_enabled("custom_draw_approach_circles")
        numbers_enabled = preview_layer_enabled("custom_draw_object_numbers")
        trail_enabled = preview_layer_enabled("custom_draw_cursor_trail")
        pulses_enabled = preview_layer_enabled("custom_draw_click_pulses")
        timeline_enabled = preview_layer_enabled("custom_draw_timeline")
        keys_enabled = preview_layer_enabled("custom_draw_key_boxes")
        header_enabled = preview_layer_enabled("custom_draw_header")
        ticks_enabled = preview_layer_enabled("custom_draw_slider_ticks")
        follow_enabled = preview_layer_enabled("custom_draw_slider_follow_circle")
        judgments_enabled = preview_layer_enabled("custom_draw_judgments")
        judgment_totals_enabled = preview_layer_enabled("custom_draw_judgment_totals")

        def sc(v: float) -> int:
            return int(round(v * aa_scale))

        def pt(x: float, y: float) -> Tuple[int, int]:
            return sc(x), sc(y)

        def bgr(hex_color: str) -> Tuple[int, int, int]:
            s = hex_color.lstrip("#")
            r, g, b = int(s[0:2], 16), int(s[2:4], 16), int(s[4:6], 16)
            return b, g, r

        def rect(img, x0, y0, x1, y1, color, thickness=-1):
            cv2.rectangle(img, pt(x0, y0), pt(x1, y1), bgr(color), thickness if thickness < 0 else max(1, sc(thickness)), cv2.LINE_AA)

        def circle(img, x, y, r, color, thickness=-1):
            cv2.circle(img, pt(x, y), sc(r), bgr(color), thickness if thickness < 0 else max(1, sc(thickness)), cv2.LINE_AA)

        def line(img, x0, y0, x1, y1, color, thickness=1):
            cv2.line(img, pt(x0, y0), pt(x1, y1), bgr(color), max(1, sc(thickness)), cv2.LINE_AA)

        def polyline(img, points, color, thickness=1):
            pts = np.array([pt(x, y) for x, y in points], dtype=np.int32).reshape((-1, 1, 2))
            cv2.polylines(img, [pts], False, bgr(color), max(1, sc(thickness)), cv2.LINE_AA)

        def put_text(img, text, x, y, color="#eeeeee", size=0.38, thickness=1, anchor="left", shadow=True):
            font = cv2.FONT_HERSHEY_SIMPLEX
            font_scale = size * aa_scale
            thick = max(1, int(round(thickness * aa_scale)))
            (tw, th), baseline = cv2.getTextSize(str(text), font, font_scale, thick)
            px, py = sc(x), sc(y)
            if anchor in ("center", "c"):
                px -= tw // 2
                py += th // 2
            elif anchor in ("right", "e", "ne", "se"):
                px -= tw
            elif anchor in ("top", "n"):
                py += th
            elif anchor == "s":
                px -= tw // 2
                py -= baseline
            elif anchor == "n":
                px -= tw // 2
                py += th
            elif anchor == "w":
                py += th // 2
            if shadow:
                cv2.putText(img, str(text), (px + sc(1), py + sc(1)), font, font_scale, bgr("#000000"), thick + sc(1), cv2.LINE_AA)
            cv2.putText(img, str(text), (px, py), font, font_scale, bgr(color), thick, cv2.LINE_AA)

        img = np.full((H, W, 3), bgr("#07070b"), dtype=np.uint8)

        if bg_enabled:
            img[:] = bgr("#10111a")
            circle(img, 100, 70, 210, "#22152e")
            circle(img, 430, 190, 235, "#0c2630")
            # Darken like the real render background does.
            dark = np.full_like(img, bgr("#07070b"))
            img = cv2.addWeighted(img, 0.38, dark, 0.62, 0)

        fx0, fy0, fw, fh = 132, 36, 340, 248
        rect(img, fx0, fy0, fx0 + fw, fy0 + fh, "#131318")
        if border_enabled:
            rect(img, fx0, fy0, fx0 + fw, fy0 + fh, "#4a4a56", thickness=1.4)

        if header_enabled:
            put_text(img, "Artist - Title [Difficulty]", 18, 24, size=0.44, thickness=1, anchor="left")
            put_text(img, "Replay: player | Song: 12.34s | Offset: 0ms", 18, 43, color="#bbbbbb", size=0.30, thickness=1, anchor="left", shadow=False)
        if judgment_totals_enabled:
            label_great = judgment_text_great_var.get().strip() or "Great"
            label_ok = judgment_text_ok_var.get().strip() or "100"
            label_meh = judgment_text_meh_var.get().strip() or "50"
            label_miss = judgment_text_miss_var.get().strip() or "Miss"
            sample_totals = f"{label_great} 314 | {label_ok} 28 | {label_meh} 6 | {label_miss} 2"
            put_text(img, sample_totals, w - 16, 43 if header_enabled else 24, color="#eeeeee", size=0.34, thickness=1, anchor="right", shadow=True)

        def sample_cubic_bezier(p0, p1, p2, p3, count=40):
            pts = []
            for i in range(count + 1):
                t = i / count
                mt = 1.0 - t
                x = (mt ** 3) * p0[0] + 3 * (mt ** 2) * t * p1[0] + 3 * mt * (t ** 2) * p2[0] + (t ** 3) * p3[0]
                y = (mt ** 3) * p0[1] + 3 * (mt ** 2) * t * p1[1] + 3 * mt * (t ** 2) * p2[1] + (t ** 3) * p3[1]
                pts.append((x, y))
            return pts

        slider_pts = sample_cubic_bezier((214, 160), (248, 104), (322, 110), (372, 178), count=48)
        slider_color = "#52caff" if visual_style_var.get().strip().lower() == "ghost" else "#36aeea"
        polyline(img, slider_pts, slider_color, thickness=21)
        polyline(img, slider_pts, "#eeeeee", thickness=3)
        if ticks_enabled:
            for idx in (12, 24, 36):
                x, y = slider_pts[idx]
                circle(img, x, y, 4.7, "#f3c54a")
                circle(img, x, y, 4.7, "#fff6cc", thickness=1.1)
        ball_x, ball_y = slider_pts[31]
        if follow_enabled:
            circle(img, ball_x, ball_y, 36, "#dddddd", thickness=1.4)
        circle(img, ball_x, ball_y, 15, slider_color)
        circle(img, ball_x, ball_y, 15, "#eeeeee", thickness=2)

        obj_x, obj_y, obj_r = 250, 196, 25
        if approach_enabled:
            circle(img, obj_x, obj_y, 68, "#aaaabe", thickness=1.5)
        fill = "#83d676" if visual_style_var.get().strip().lower() == "ghost" else "#58c958"
        circle(img, obj_x, obj_y, obj_r, fill)
        circle(img, obj_x, obj_y, obj_r, "#eeeeee", thickness=2)
        if numbers_enabled:
            put_text(img, "5", obj_x, obj_y, color="#ffffff", size=0.62, thickness=2, anchor="center")

        if trail_enabled:
            for i, (x, y) in enumerate([(166, 220), (179, 215), (193, 209), (207, 204), (222, 199)]):
                circle(img, x, y, 2.2 + i * 0.9, "#9d9da5")

        cursor_x, cursor_y = 222, 199
        circle(img, cursor_x, cursor_y, 8, "#ffffff", thickness=1.8)
        circle(img, cursor_x, cursor_y, 3.1, "#ffffff")
        line(img, cursor_x - 15, cursor_y, cursor_x - 6, cursor_y, "#ffffff", thickness=0.9)
        line(img, cursor_x + 6, cursor_y, cursor_x + 15, cursor_y, "#ffffff", thickness=0.9)
        line(img, cursor_x, cursor_y - 15, cursor_x, cursor_y - 6, "#ffffff", thickness=0.9)
        line(img, cursor_x, cursor_y + 6, cursor_x, cursor_y + 15, "#ffffff", thickness=0.9)

        if pulses_enabled:
            circle(img, cursor_x, cursor_y, 36, "#ff7a1a", thickness=2.5)
            put_text(img, "A", cursor_x + 24, cursor_y - 18, color="#ff9a35", size=0.48, thickness=2, anchor="left")

        if judgments_enabled:
            label = preview_judgment_label()
            tx, ty, anchor_name = preview_text_position(obj_x, obj_y, obj_r, len(label) * 8)
            cv_anchor = "center" if anchor_name == "center" else anchor_name
            color = "#76dfff" if label != (judgment_text_miss_var.get().strip() or "Miss") else "#ff6060"
            put_text(img, label, tx, ty, color=color, size=0.58, thickness=2, anchor=cv_anchor)
            if judgment_draw_miss_x_var.get():
                miss_x, miss_y = obj_x + 76, obj_y - 43
                line(img, miss_x - 13, miss_y - 13, miss_x + 13, miss_y + 13, "#ff6060", thickness=2.8)
                line(img, miss_x - 13, miss_y + 13, miss_x + 13, miss_y - 13, "#ff6060", thickness=2.8)
            if judgment_show_slider_details_var.get():
                put_text(img, "Break | 2/3 ticks", obj_x, obj_y - obj_r - 30, color="#ff6060", size=0.30, thickness=1, anchor="center")

        if keys_enabled:
            kx, ky = 18, h - 48
            for i, (label, held, color) in enumerate([("A", True, "#ff7800"), ("D", False, "#00beff")]):
                x0 = kx + i * 50
                rect(img, x0, ky, x0 + 40, ky + 30, "#46b85a" if held else "#424242")
                rect(img, x0, ky, x0 + 40, ky + 30, color, thickness=1.5)
                put_text(img, label, x0 + 20, ky + 15, color="#ffffff", size=0.48, thickness=2, anchor="center")

        if timeline_enabled:
            x0, y0, x1, y1 = 132, h - 50, w - 18, h - 20
            rect(img, x0, y0, x1, y1, "#17171d")
            rect(img, x0, y0, x1, y1, "#44444c", thickness=1)
            put_text(img, "A", x0 + 8, y0 + 10, color="#ff7800", size=0.24, thickness=1, anchor="left", shadow=False)
            put_text(img, "D", x0 + 8, y0 + 24, color="#00beff", size=0.24, thickness=1, anchor="left", shadow=False)
            line(img, x1 - 12, y0 + 4, x1 - 12, y1 - 4, "#ffffff", thickness=1.5)
            for x, y, color in [(x1 - 130, y0 + 10, "#ff7800"), (x1 - 82, y0 + 24, "#00beff"), (x1 - 44, y0 + 10, "#ff7800")]:
                circle(img, x, y, 4, color)

        put_text(img, f"Mode: {performance_var.get().strip().lower()}", w - 10, 16, color="#dddddd", size=0.30, thickness=1, anchor="right", shadow=True)

        preview_img = cv2.resize(img, (w, h), interpolation=cv2.INTER_AREA)
        preview_rgb = cv2.cvtColor(preview_img, cv2.COLOR_BGR2RGB)
        ok, encoded_png = cv2.imencode(".png", preview_rgb)
        if ok:
            data = base64.b64encode(encoded_png.tobytes()).decode("ascii")
            photo = tk.PhotoImage(data=data, format="png")
            c._preview_photo = photo
            c.create_image(0, 0, image=photo, anchor="nw")
        else:
            c.create_text(w // 2, h // 2, text="Preview unavailable", fill="#eeeeee")

    tk.Button(preview_frame, text="Refresh preview", command=update_preview).grid(row=1, column=1, sticky="e", padx=(4, 8), pady=(0, 8))

    preview_vars = [
        visual_style_var, performance_var, judgment_show_great_var, judgment_text_great_var,
        judgment_text_ok_var, judgment_text_meh_var, judgment_text_miss_var,
        judgment_position_var, judgment_offset_x_var, judgment_offset_y_var,
        judgment_draw_miss_x_var, judgment_show_slider_details_var,
    ]
    for var in preview_vars:
        try:
            var.trace_add("write", update_preview)
        except Exception:
            pass
    for var in custom_visual_vars.values():
        try:
            var.trace_add("write", update_preview)
        except Exception:
            pass
    root.after(100, update_preview)

    fps_desc_var = tk.StringVar()
    resolution_desc_var = tk.StringVar()
    parallel_desc_var = tk.StringVar()
    snake_desc_var = tk.StringVar()
    visual_style_desc_var = tk.StringVar()
    quality_desc_var = tk.StringVar()

    def update_setting_descriptions(*_):
        fps_choice = fps_var.get().strip()
        if fps_choice.lower().startswith("auto"):
            fps_desc_var.set(f"Auto uses detected refresh ({detected_fps} Hz); FPS is frames rendered per second.")
        else:
            try:
                fps_value = int(fps_choice.split()[0])
                fps_desc_var.set(f"Renders {fps_value} frames per second; higher values create more frames to process.")
            except Exception:
                fps_desc_var.set("Frames rendered per second; higher values create more frames to process.")

        resolution_choice = resolution_var.get().strip()
        if resolution_choice.lower().startswith("auto"):
            resolution_desc_var.set(f"Auto uses detected size ({detected_width}x{detected_height}); resolution sets pixels per frame.")
        else:
            resolution_desc_var.set(f"Outputs at {resolution_choice}; larger sizes increase per-frame render work.")

        parallel_choice = parallel_var.get().strip()
        if parallel_choice.lower() == "auto":
            parallel_desc_var.set("Auto chooses worker count; workers split silent video rendering into chunks.")
        elif parallel_choice == "1":
            parallel_desc_var.set("Uses one render process; avoids chunking overhead and shared-resource contention.")
        else:
            parallel_desc_var.set(f"Uses {parallel_choice} render processes; more workers share disk and encoder resources.")

        snake_desc_var.set("Slider reveal lead time; smaller reveals later, larger reveals earlier.")

        style_choice = visual_style_var.get().strip().lower()
        if style_choice == "solid":
            visual_style_desc_var.set("Solid draws more opaque objects; ghost draws more transparent objects.")
        else:
            visual_style_desc_var.set("Ghost draws more transparent objects; solid draws more opaque objects.")

        quality_choice = quality_var.get().strip().lower()
        if quality_choice == "fast":
            quality_desc_var.set("Fast uses lighter encoder settings for quicker encoding.")
        elif quality_choice == "balanced":
            quality_desc_var.set("Balanced trades some speed for cleaner compression.")
        elif quality_choice == "high":
            quality_desc_var.set("High preserves more detail with more encoding work.")
        elif quality_choice == "max":
            quality_desc_var.set("Max uses strongest quality settings and highest encoding work.")
        else:
            quality_desc_var.set("Encoder quality profile; higher settings can increase file size and encode work.")

    def desc_label(row: int, var: tk.StringVar):
        tk.Label(main_frame, textvariable=var, anchor="w", justify="left", wraplength=900).grid(row=row, column=2, columnspan=2, sticky="w", pady=4)

    row_label(8, "FPS / Hz")
    ttk.Combobox(main_frame, textvariable=fps_var, values=fps_choices, state="readonly").grid(row=8, column=1, sticky="ew", pady=4)
    desc_label(8, fps_desc_var)

    row_label(9, "Resolution")
    ttk.Combobox(main_frame, textvariable=resolution_var, values=resolution_choices, state="readonly").grid(row=9, column=1, sticky="ew", pady=4)
    desc_label(9, resolution_desc_var)

    row_label(10, "Parallel workers")
    ttk.Combobox(main_frame, textvariable=parallel_var, values=["Auto", "1", "2", "3", "4", "5", "6", "7", "8"], state="readonly").grid(row=10, column=1, sticky="ew", pady=4)
    desc_label(10, parallel_desc_var)

    row_label(11, "Snake-in duration ms")
    tk.Entry(main_frame, textvariable=snake_var).grid(row=11, column=1, sticky="ew", pady=4)
    desc_label(11, snake_desc_var)

    row_label(12, "Visual style")
    ttk.Combobox(main_frame, textvariable=visual_style_var, values=["ghost", "solid"], state="readonly").grid(row=12, column=1, sticky="ew", pady=4)
    desc_label(12, visual_style_desc_var)

    row_label(13, "Quality")
    ttk.Combobox(main_frame, textvariable=quality_var, values=quality_choices, state="readonly").grid(row=13, column=1, sticky="ew", pady=4)
    desc_label(13, quality_desc_var)

    for var in (fps_var, resolution_var, parallel_var, snake_var, visual_style_var, quality_var):
        try:
            var.trace_add("write", update_setting_descriptions)
        except Exception:
            pass
    update_setting_descriptions()

    row_label(14, "Output folder")
    tk.Entry(main_frame, textvariable=output_dir_var).grid(row=14, column=1, columnspan=2, sticky="ew", pady=4)
    tk.Button(main_frame, text="Browse", command=lambda: browse_dir(output_dir_var)).grid(row=14, column=3, sticky="ew", padx=(6, 0))

    row_label(15, "osu! root folder")
    tk.Entry(main_frame, textvariable=osu_root_var).grid(row=15, column=1, columnspan=2, sticky="ew", pady=4)
    tk.Button(main_frame, text="Browse", command=lambda: browse_dir(osu_root_var)).grid(row=15, column=3, sticky="ew", padx=(6, 0))

    row_label(16, "replay/export folder")
    tk.Entry(main_frame, textvariable=exports_var).grid(row=16, column=1, columnspan=2, sticky="ew", pady=4)
    tk.Button(main_frame, text="Browse", command=lambda: browse_dir(exports_var)).grid(row=16, column=3, sticky="ew", padx=(6, 0))

    row_label(17, "specific replay .osr optional")
    tk.Entry(main_frame, textvariable=replay_var).grid(row=17, column=1, columnspan=2, sticky="ew", pady=4)
    tk.Button(main_frame, text="Browse", command=lambda: browse_file(replay_var, [("osu replay", "*.osr"), ("All files", "*.*")])).grid(row=17, column=3, sticky="ew", padx=(6, 0))

    row_label(18, "specific beatmap .osu optional")
    tk.Entry(main_frame, textvariable=beatmap_var).grid(row=18, column=1, columnspan=2, sticky="ew", pady=4)
    tk.Button(main_frame, text="Browse", command=lambda: browse_file(beatmap_var, [("osu beatmap", "*.osu"), ("All files", "*.*")])).grid(row=18, column=3, sticky="ew", padx=(6, 0))

    tk.Label(main_frame, text="Use 'Start Render Now' for the newest existing replay, or 'Watch Exports + Auto Render' to wait for a new export.", anchor="w").grid(row=20, column=0, columnspan=4, sticky="w", pady=(10, 2))

    output_box = tk.Text(main_frame, height=32, wrap="word", bg="#111111", fg="#eeeeee", insertbackground="#eeeeee")
    output_box.grid(row=22, column=0, columnspan=4, sticky="nsew", pady=(12, 0))

    scrollbar = tk.Scrollbar(main_frame, command=output_box.yview)
    scrollbar.grid(row=22, column=4, sticky="ns", pady=(12, 0))
    output_box.configure(yscrollcommand=scrollbar.set)

    main_frame.grid_columnconfigure(1, weight=1)
    main_frame.grid_columnconfigure(2, weight=1)
    main_frame.grid_rowconfigure(22, weight=1)

    def append_log(text: str):
        output_box.insert("end", text)
        output_box.see("end")
        root.update_idletasks()

    def collect_cfg() -> Dict:
        new_cfg = dict(DEFAULT_CONFIG)
        new_cfg.update(cfg)
        new_cfg["visual_style"] = visual_style_var.get().strip().lower()
        new_cfg["osu_install_type"] = "stable" if install_type_var.get().strip().lower() in ("osu", "osu!") else "lazer"
        new_cfg["miss_sheet_scale"] = MISS_SHEET_SCALE
        # Encoder is kept on NVENC in the simplified UI; Quality remains user-selectable.
        new_cfg["render_encoder"] = "h264_nvenc"
        selected_quality = quality_var.get().strip().lower()
        new_cfg["quality_profile"] = selected_quality if selected_quality in quality_choices else "fast"
        new_cfg["performance_mode"] = performance_var.get().strip().lower()
        for _, key in custom_visual_options:
            new_cfg[key] = bool(custom_visual_vars[key].get())

        new_cfg["judgment_show_great"] = bool(judgment_show_great_var.get())
        new_cfg["judgment_text_great"] = judgment_text_great_var.get()
        new_cfg["judgment_text_ok"] = judgment_text_ok_var.get()
        new_cfg["judgment_text_meh"] = judgment_text_meh_var.get()
        new_cfg["judgment_text_miss"] = judgment_text_miss_var.get()
        try:
            new_cfg["judgment_text_duration_ms"] = max(50, int(judgment_duration_var.get()))
        except ValueError:
            new_cfg["judgment_text_duration_ms"] = 300
        pos = judgment_position_var.get().strip().lower()
        new_cfg["judgment_text_position"] = pos if pos in ("center", "above", "below", "left", "right") else "center"
        try:
            new_cfg["judgment_text_offset_x"] = int(judgment_offset_x_var.get())
        except ValueError:
            new_cfg["judgment_text_offset_x"] = 0
        try:
            new_cfg["judgment_text_offset_y"] = int(judgment_offset_y_var.get())
        except ValueError:
            new_cfg["judgment_text_offset_y"] = 0
        new_cfg["judgment_draw_miss_x"] = bool(judgment_draw_miss_x_var.get())
        new_cfg["judgment_show_slider_details"] = bool(judgment_show_slider_details_var.get())

        if parallel_var.get().strip().lower() == "auto":
            new_cfg["parallel_workers"] = 0
        else:
            try:
                new_cfg["parallel_workers"] = int(parallel_var.get().strip())
            except ValueError:
                new_cfg["parallel_workers"] = 0
        fps_choice = fps_var.get().strip()
        if fps_choice.lower().startswith("auto"):
            new_cfg["render_fps"] = 0
        else:
            try:
                new_cfg["render_fps"] = int(fps_choice.split()[0])
            except ValueError:
                new_cfg["render_fps"] = 0

        resolution_choice = resolution_var.get().strip()
        if resolution_choice.lower().startswith("auto"):
            new_cfg["render_width"] = 0
            new_cfg["render_height"] = 0
        else:
            try:
                w_text, h_text = resolution_choice.lower().split("x", 1)
                new_cfg["render_width"] = int(w_text.strip())
                new_cfg["render_height"] = int(h_text.strip())
            except ValueError:
                new_cfg["render_width"] = 0
                new_cfg["render_height"] = 0
        new_cfg["watch_exports_on_start"] = False
        new_cfg["output_dir"] = output_dir_var.get().strip() or "osu_visualizer_output"
        new_cfg["exports_dir"] = exports_var.get().strip()
        new_cfg["osu_root_dir"] = osu_root_var.get().strip()
        new_cfg["replay_path"] = replay_var.get().strip()
        new_cfg["beatmap_path"] = beatmap_var.get().strip()
        new_cfg["generate_miss_sheet"] = bool(miss_sheet_var.get())
        new_cfg["generate_data_sheet"] = bool(data_sheet_var.get())
        new_cfg["enable_start_ui"] = True
        try:
            new_cfg["snake_in_duration_ms"] = int(snake_var.get())
        except ValueError:
            new_cfg["snake_in_duration_ms"] = 450
        new_cfg.pop("parallel_chunk_seconds", None)
        return new_cfg

    def save_only():
        try:
            save_config_file(collect_cfg())
            messagebox.showinfo("Saved", "Config saved:" + NL + CONFIG_PATH)
        except Exception as exc:
            messagebox.showerror("Save failed", str(exc))

    def open_output_folder():
        folder = Path(output_dir_var.get().strip() or BASE_OUTPUT_DIR or "osu_visualizer_output").expanduser()
        folder.mkdir(parents=True, exist_ok=True)
        if os.name == "nt":
            os.startfile(os.path.abspath(folder))
        else:
            subprocess.Popen(["xdg-open", os.path.abspath(folder)])

    def run_render(watch_mode: bool = False):
        try:
            cfg_to_save = collect_cfg()
            cfg_to_save["watch_exports_on_start"] = bool(watch_mode)
            save_config_file(cfg_to_save)
        except Exception as exc:
            messagebox.showerror("Could not save config", str(exc))
            return

        start_button.config(state="disabled")
        watch_button.config(state="disabled")
        append_log(NL + "Starting render..." + NL)
        cmd = [sys.executable, "-u", os.path.abspath(__file__), "--no-ui"]
        if watch_mode:
            cmd.append("--watch")

        def worker():
            try:
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
                if proc.stdout:
                    for line in proc.stdout:
                        root.after(0, append_log, line)
                ret = proc.wait()
                root.after(0, append_log, NL + f"Render process exited with code {ret}" + NL)
            except Exception as exc:
                root.after(0, append_log, NL + f"ERROR launching render: {exc}" + NL)
            finally:
                root.after(0, lambda: start_button.config(state="normal"))
                root.after(0, lambda: watch_button.config(state="normal"))

        threading.Thread(target=worker, daemon=True).start()

    button_frame = tk.Frame(main_frame)
    button_frame.grid(row=21, column=0, columnspan=4, sticky="ew", pady=(4, 0))
    start_button = tk.Button(button_frame, text="Start Render Now", command=lambda: run_render(False), height=2, bg="#2d7d46", fg="white")
    start_button.pack(side="left", padx=(0, 8))
    watch_button = tk.Button(button_frame, text="Watch Exports + Auto Render", command=lambda: run_render(True), height=2, bg="#345f9e", fg="white")
    watch_button.pack(side="left", padx=(0, 8))
    tk.Button(button_frame, text="Save Config", command=save_only, height=2).pack(side="left", padx=(0, 8))
    tk.Button(button_frame, text="Open Output Folder", command=open_output_folder, height=2).pack(side="left", padx=(0, 14))
    tk.Checkbutton(button_frame, text="Generate miss snapshot sheet", variable=miss_sheet_var).pack(side="left", padx=(0, 12))
    tk.Checkbutton(button_frame, text="Generate data sheet CSV/HTML", variable=data_sheet_var).pack(side="left")

    append_log(f"Config: {CONFIG_PATH}" + NL)
    append_log(f"osu! type: {install_type_var.get()}" + NL)
    append_log(f"Detected osu folder: {OSU_FOLDER or 'not found'}" + NL)
    append_log(f"Detected replay/export folder: {REPLAY_FOLDER or 'not found'}" + NL)
    append_log(f"Output root: {output_dir_var.get() or BASE_OUTPUT_DIR}" + NL)
    append_log(f"Performance mode: {performance_var.get()}" + NL)
    append_log(f"Quality: {quality_var.get()}" + NL)
    append_log("Each render will create a named subfolder for that replay/map inside the output folder." + NL)
    if normalize_osu_install_type(install_type_var.get()) == "stable":
        append_log("For osu!, use the Replays folder and osu! root with Songs; specific .osr/.osu paths also work." + NL)
    else:
        append_log("For osu!(lazer), put exported .osr replay and matching .osz beatmap in the exports folder." + NL)

    root.mainloop()


def run_chunk_renderer_main() -> None:
    start_text = cli_arg_value("--chunk-start")
    end_text = cli_arg_value("--chunk-end")
    out_path = cli_arg_value("--chunk-out")
    offset_text = cli_arg_value("--replay-to-song-offset", "0")

    if not start_text or not end_text or not out_path:
        raise RuntimeError("Chunk render requires --chunk-start, --chunk-end, and --chunk-out.")

    chunk_start = int(start_text)
    chunk_end = int(end_text)
    replay_to_song_offset = int(offset_text or "0")

    osu_folder = find_osu_folder()
    replay_path = Path(REPLAY_PATH).expanduser()
    if not replay_path.exists():
        raise FileNotFoundError(f"Chunk replay path does not exist: {replay_path}")

    replay = Replay.from_path(replay_path)
    frames, clicks = parse_replay_frames(replay)

    beatmap = None
    beatmap_path = Path(BEATMAP_PATH).expanduser() if BEATMAP_PATH else None
    if beatmap_path and beatmap_path.exists():
        beatmap = parse_beatmap(beatmap_path)
    else:
        found = find_beatmap(osu_folder, replay.beatmap_hash, replay_path)
        if found:
            beatmap = parse_beatmap(found)

    Renderer(frames, clicks, beatmap, replay, replay_to_song_offset, replay_path=replay_path).render_silent_range(
        out_path,
        chunk_start,
        chunk_end,
        progress_label=f"chunk {chunk_start}-{chunk_end}ms",
    )


def run_console_main() -> None:
    try:
        global REPLAY_PATH
        if "--render-chunk" in sys.argv:
            run_chunk_renderer_main()
            return
        if "--watch" in sys.argv:
            watched_replay = wait_for_new_exports()
            if watched_replay is not None:
                REPLAY_PATH = str(watched_replay)
        main()
    except Exception as exc:
        print()
        print("ERROR:", exc)
        print()
        print("Troubleshooting:")
        print("- Install: pip install osrparse opencv-python numpy imageio-ffmpeg")
        print("- Keep the replay .osr in the configured exports folder.")
        print("- If .osz is missing, use the guided osu!lazer export flow: right-click map > Edit > File > Export > For compatibility (.osz).")
        print("- If the wrong replay is used, set replay_path in osu_visualizer_config.json.")
        print("- If beatmap matching fails, set beatmap_path to the extracted .osu file.")
        print("- If clicks are globally early/late, adjust MANUAL_REPLAY_TO_SONG_OFFSET_MS.")
        print("- If NVENC fails, set render_encoder to libx264 in osu_visualizer_config.json.")
        sys.exit(1)


if __name__ == "__main__":
    if ENABLE_START_UI and "--no-ui" not in sys.argv:
        start_ui()
    else:
        run_console_main()
