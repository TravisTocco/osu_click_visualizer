# osu! Replay Visualizer & Miss Analyzer

A Python tool for rendering osu! / osu!(lazer) replay files into annotated videos.

It visualizes cursor movement, hit objects, sliders, judgments, misses, timing, and optional analysis outputs. The goal is to make replay review easier by showing not only what happened, but where and when it happened.

---

## Features

- Supports both **osu! stable** and **osu!(lazer)** replay workflows
- Renders annotated replay videos from `.osr` files
- Customizable visual layers:
  - background
  - playfield border
  - approach circles
  - object numbers
  - cursor trail
  - click pulses
  - timeline
  - key boxes
  - slider ticks
  - slider follow circle
  - judgment popups
- Adjustable FPS, resolution, visual style, quality, and parallel workers
- Fast parallel rendering with progress updates and ETA logging
- Live visual preview of selected visual settings
- Organized output folders named after the replay/map
- Optional miss snapshot sheet
- Optional CSV/HTML replay data sheet

---

## Requirements

Install Python 3.10 or newer.

Then install the required packages:

```bash
pip install osrparse opencv-python numpy imageio-ffmpeg
```

---

## Basic Usage

Run the script:

```bash
python osu_replay_click_visualizer.py
```

The UI will open.

From there:

1. Choose whether you are using **osu!** or **osu!(lazer)**.
2. Confirm your replay/export folder.
3. Confirm your osu! root folder.
4. Choose your visual/render settings.
5. Click **Start Render Now**.

The script will use the newest replay in the selected replay/export folder unless you manually select a specific `.osr` file.

---

## osu!(lazer) Workflow

For osu!(lazer), export the replay and beatmap through lazer.

Recommended flow:

1. Right-click the beatmap.
2. Choose **Edit**.
3. Use **File > Export > For compatibility (.osz)**.
4. Put the exported `.osr` replay and matching `.osz` beatmap in the configured exports folder.
5. Start the render.

The script will extract and match the beatmap automatically when possible.

---

## osu! Stable Workflow

For osu! stable, use the stable replay folder and Songs folder.

Replays are usually found in:

```text
osu!/Replays
```

Beatmaps are usually found in:

```text
osu!/Songs
```

Select **osu!** as the type in the UI. The script will search the stable Songs folder for the matching `.osu` beatmap.

---

## Output

Each render creates a named output folder containing files such as:

- rendered video
- silent temporary video
- miss snapshot sheet
- individual miss frames
- CSV/HTML replay data
- chunk files used during rendering
- render logs

Output folders are named using the beatmap, replay player, and render timestamp.

---

## Render Settings

### FPS / Hz

Controls how many frames are rendered per second. Higher values create smoother video and more temporal detail, but increase render time.

### Resolution

Controls the output video size. Higher resolutions preserve more detail but require more drawing, memory, encoding, and disk work.

### Parallel Workers

Controls how many render workers are used. More workers can improve render speed, but also share CPU, disk, memory, and encoder resources.

### Snake-in Duration

Controls how long sliders take to visually reveal before their hit time.

### Visual Style

Controls the overall look of the rendered objects.

### Quality

Controls encoding quality and compression. Higher quality can preserve more detail, but usually increases encoding cost and file size.

---

## Miss Snapshot Sheet

When enabled, the script creates a miss snapshot sheet showing the key moment for each miss.

Each tile includes:

- miss number
- object number
- object type
- timestamp
- visual snapshot of the miss moment

This is useful for quickly reviewing repeated aim, timing, slider, or reading mistakes.

---

## Data Sheet

When enabled, the script can export replay data as CSV/HTML.

This can be useful for deeper review, spreadsheet analysis, or keeping a record of miss/judgment information.

---

## Performance Notes

Rendering speed depends heavily on:

- selected FPS
- selected resolution
- selected quality
- enabled visual layers
- CPU performance
- GPU encoder performance
- disk speed
- number of parallel workers

If rendering feels slow, try reducing FPS, reducing resolution, lowering quality, or disabling extra visual layers in Custom mode.

---

## Troubleshooting

### The script cannot find the beatmap

Set the specific `.osu` file manually in the UI.

### The wrong replay is used

Set the specific `.osr` replay manually in the UI.

### Clicks feel globally early or late

Adjust the replay/song offset setting in the config.

### NVENC fails

Your GPU or drivers may not support NVENC. Use a different quality/encoder configuration if needed.

### The render is slow

Try reducing FPS, reducing resolution, lowering quality, or disabling visual layers.

---

## License / Usage

This project is free to use, modify, share, and distribute.

You may:

- use it personally
- modify it
- redistribute it
- include it in your own projects
- share edited versions
- use it for videos, analysis, or other osu! replay-related work

No warranty is provided. Use it at your own risk.

This tool does not modify your osu! installation, replay files, or beatmaps.

---

## Disclaimer

This project was basically **100% vibe coded with ChatGPT 5.5**.

That means it was built through iterative prompting, testing, debugging, and refinement rather than through a traditional software development process. It works for the intended workflow it was tested around, but there may still be bugs, edge cases, messy internals, or behavior that needs cleanup.

Contributions, fixes, improvements, and rewrites are welcome.

---

## Credits

Created for osu! replay review and visualization.

osu! is owned by its respective creators. This project is unofficial and is not affiliated with or endorsed by osu!, ppy, or osu!(lazer).
