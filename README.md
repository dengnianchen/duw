# duw — disk usage, web

A fast, headless-friendly disk-usage visualizer for Linux servers, in the spirit of
[baobab](https://apps.gnome.org/Baobab/) and [duc](https://duc.zevv.nl/) — but
served over HTTP so you can use it on remote boxes without a desktop.

Point it at a directory, get an interactive sunburst plus per-directory
breakdown. Drilling into any subdirectory is **instant** — the initial scan is
kept in memory and every drill-down is a slice of that tree, not a re-scan.

- Single Python file, no dependencies (stdlib + vanilla JS/SVG in the page).
- Parallel first scan (uses every core available).
- Sizes match `du` (uses `st_blocks * 512`, counts directory inodes).
- **Symlinks are never followed or counted** — for files or directories.
- Gzip cache under `~/.cache/duw/` so restarts are instant.

## Screenshot

Left: sunburst (click a slice to dive in, click the center or press `Esc` to go
up). Right: subdirectories + files in the focused directory, with proportion
bars and percentages.

## Requirements

- Python 3.8+
- Linux (relies on `st_blocks`; should work on any POSIX filesystem)

## Usage

```bash
# Start the server; pick the root from the web page
python3 duw.py

# Or pass a root at startup (scan runs in the background, page opens once ready)
python3 duw.py /data

# Bind to all interfaces + custom port for remote access
python3 duw.py --host 0.0.0.0 --port 8000

# Force a fresh scan, ignoring any cached index
python3 duw.py --rebuild /data
```

Then open `http://<host>:<port>/` in a browser.

### In the web UI

- Enter a path and click **Analyze** — first scan runs in the background with
  live progress (dirs scanned, elapsed time).
- Click **Rebuild** to ignore the cache and rescan.
- Click a slice, a subdirectory row, or a breadcrumb to navigate.
- Click the sunburst's center, the `↑ up` link, or press `Esc` / `Backspace`
  to go up one level.
- Click the `duw` logo to return to the landing page and analyze a different
  directory.
- Click `rescan` (bottom right) to rebuild the current root.

## Right-panel display rules

- **Subdirectories** — the title shows the total count. If there are ≤10, all
  are listed. Otherwise: every subdirectory ≥1 GiB is shown; if that's fewer
  than 10, the largest sub-1 GiB entries fill the list up to 10; the rest are
  aggregated into a single `(others)` row.
- **Files** — the title shows the total file count. If there are ≤20, all are
  listed. Otherwise: every file ≥100 MiB is shown; if that's fewer than 10,
  the largest sub-100 MiB files fill the list up to 10; the rest are
  aggregated into `(others)`.

Bar widths and percentages are relative to the current directory's total size,
so bars and text always agree.

## Speed model

- **First scan** is parallel — Python multiprocessing spawns one worker per
  top-level subtree (capped at CPU count) and `os.scandir` walks each subtree
  sequentially inside the worker.
- **All later drill-downs** slice the in-memory tree — no filesystem access,
  microsecond latency.
- **File listings** (right panel) are a single non-recursive `os.scandir` of
  the focused directory — fast, and always current, so file counts reflect
  reality even if the cached index is stale.

To keep JSON payloads bounded on very large trees (millions of directories),
`/api/subtree` uses breadth-first expansion with a per-node child cap (300)
and a total node budget (8000). Every ring keeps exact proportions because
the overflow is aggregated into an `(other)` slice that carries the full
missing size.

## Cache

Indexes are written to `~/.cache/duw/<hash>.json.gz`, keyed by the absolute
root path. On startup or `Analyze`, a matching cache is loaded instantly
(saves you the initial scan). Use `--rebuild` on the CLI or the **Rebuild**
button in the UI to force a fresh scan.

A known limitation: caches are keyed per root, so analyzing `/foo` does not
speed up a later analysis of `/foo/bar`, and rescanning a subdirectory does
not update its ancestors' caches. Future versions may unify this.

## HTTP API

The web page uses these endpoints; they're stable enough to script against:

| Endpoint | Purpose |
|---|---|
| `GET /api/status` | Current scan status (idle / scanning / ready / error), progress counters, root path, total size and file count |
| `GET /api/scan?path=<abs>&rebuild=0\|1` | Trigger a background scan of `path` |
| `GET /api/subtree?path=<abs>&depth=<n>` | Depth-bounded subtree JSON for the sunburst |
| `GET /api/files?path=<abs>` | Direct files in `path` with the `(others)` aggregation applied |
| `GET /api/root` | Root path, size, and file count |
| `GET /api/suggest` | A default path for the landing input (`$HOME`) |

## CLI reference

```
usage: duw.py [-h] [--host HOST] [--port PORT] [--db DB] [--rebuild] [root]

positional arguments:
  root         directory to analyze at startup (optional)

options:
  --host HOST  bind address (default 127.0.0.1)
  --port PORT  bind port (default 8765)
  --db PATH    override the cache file location
  --rebuild    force a fresh index of the given root
```

## Notes and caveats

- Symlinks are neither followed nor counted, matching `du -P`.
- Reported sizes are disk allocation (`st_blocks * 512`), matching `du`'s
  default output — not apparent size (which is what `du -b` shows).
- Hard links to the same inode are counted every time they're encountered
  (same behavior as `du` without `-l`, minus the dedup pass).
- Access-denied directories are silently skipped.
- The server has no authentication. If you bind to `0.0.0.0` on a shared
  host, put it behind an SSH tunnel or a reverse proxy with auth.

## License

MIT
