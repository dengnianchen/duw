#!/usr/bin/env python3
"""
duw — disk usage, web.  A baobab/duc-like disk-usage visualizer for headless boxes.

Usage:
    duw.py                      # serve, pick the root from the web page
    duw.py /data                # index /data at startup, then serve
    duw.py --host 0.0.0.0 --port 8000
    duw.py --rebuild /data      # CLI: force a fresh index of /data

Web UI:
    * Type a path, click Analyze (or Rebuild to force a re-scan).
    * First scan runs in a background thread with live progress (dirs scanned,
      elapsed).  Subsequent drill-downs are slices of the in-memory tree —
      instantaneous, no re-scan.

Speed model:
    * One parallel walk (dirs only) builds an in-memory tree, using
      st_blocks*512 so numbers match `du`. Symlinks are never followed or
      counted. Up to 256-way parallelism for the initial scan.
    * All scans merge into ONE shared on-disk cache (~/.cache/duw/tree.json.gz):
        - analyze an upper dir -> lower dirs are already in the forest and
          reused instantly (no re-scan);
        - re-scan (Rebuild) a lower dir -> its new totals propagate up to every
          scanned ancestor.
    * The server keeps the forest in RAM; every drill-down is a slice -> instant.
    * File-level detail for the focused directory is a live single-dir scan
      (one directory, not recursive) -> fast.
"""
import argparse
import gzip
import hashlib
import html
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import multiprocessing as mp

sys.setrecursionlimit(1_000_000)

# --------------------------------------------------------------------------- #
#  Indexing
# --------------------------------------------------------------------------- #

_COUNTER = None          # mp.Value (lockless) for live progress, or None


def _inc():
    global _COUNTER
    if _COUNTER is not None:
        try:
            _COUNTER.value += 1     # lockless; lost increments are fine for a counter
        except Exception:
            pass


def _size_of(st):
    """Actual on-disk usage in bytes (matches `du`), fallback to apparent size."""
    bs = st.st_blocks * 512 if getattr(st, "st_blocks", 0) else 0
    return bs if bs > 0 else st.st_size


def recompute_node(n):
    """Recompute recursive size/count from self_* + children (children must be
    correct already). Used bottom-up after a subtree changes."""
    n["size"] = n["self_size"] + sum(c["size"] for c in n["children"])
    n["count"] = n["self_count"] + sum(c["count"] for c in n["children"])


def scan_dir(path):
    """Recursively scan one directory, returning a node dict.

    Node = {name, path, self_size, self_count, size, count, children}.
    self_size = this dir's inode + its direct files' disk usage; size/count are
    recursive totals. Symlinks are neither followed nor counted.
    """
    _inc()
    self_size = 0
    self_count = 0
    try:
        self_size += _size_of(os.stat(path, follow_symlinks=False))  # dir inode
    except OSError:
        pass
    dirs = []
    try:
        it = os.scandir(path)
    except (PermissionError, OSError):
        it = None
    if it is not None:
        for e in it:
            try:
                if e.is_dir(follow_symlinks=False):
                    dirs.append(e.path)
                elif e.is_file(follow_symlinks=False):
                    try:
                        self_size += _size_of(e.stat(follow_symlinks=False))
                        self_count += 1
                    except OSError:
                        pass
            except OSError:
                continue
    children = [scan_dir(d) for d in dirs]
    children.sort(key=lambda x: x["size"], reverse=True)
    node = {"name": os.path.basename(path) or path, "path": path,
            "self_size": self_size, "self_count": self_count,
            "size": 0, "count": 0, "children": children}
    recompute_node(node)
    return node


def scan_self(path):
    """Stat a directory's own inode + direct files (no recursion).
    Returns (self_size, self_count, [subdir_paths])."""
    self_size = 0
    try:
        self_size += _size_of(os.stat(path, follow_symlinks=False))
    except OSError:
        pass
    self_count = 0
    dirs = []
    try:
        it = os.scandir(path)
    except (PermissionError, OSError):
        return self_size, self_count, []
    for e in it:
        try:
            if e.is_dir(follow_symlinks=False):
                dirs.append(e.path)
            elif e.is_file(follow_symlinks=False):
                try:
                    self_size += _size_of(e.stat(follow_symlinks=False))
                    self_count += 1
                except OSError:
                    pass
        except OSError:
            continue
    return self_size, self_count, dirs


def parallel_scan(fresh_dirs):
    """Scan a list of directories in parallel; returns nodes in order."""
    if not fresh_dirs:
        return []
    nproc = min(len(fresh_dirs), mp.cpu_count() or 1)
    if nproc > 1 and len(fresh_dirs) > 1:
        with mp.Pool(nproc) as pool:
            return list(pool.map(scan_dir, fresh_dirs))
    return [scan_dir(d) for d in fresh_dirs]


def build_tree_reuse(root, force):
    """Build a node for `root`.  When not forced, top-level subdirs already
    present in the merged tree (NODES) are reused as-is instead of re-scanned;
    the rest are scanned in parallel.  This lets "analyze an upper dir after a
    lower one" skip the lower one's subtree."""
    root = os.path.abspath(root)
    self_size, self_count, subdir_paths = scan_self(root)
    reuse, fresh = [], []
    for d in subdir_paths:
        if (not force) and d in NODES:
            reuse.append(d)
        else:
            fresh.append(d)
    fresh_nodes = parallel_scan(fresh)
    children = [NODES[d] for d in reuse] + fresh_nodes
    children.sort(key=lambda x: x["size"], reverse=True)
    node = {"name": os.path.basename(root) or root, "path": root,
            "self_size": self_size, "self_count": self_count,
            "size": 0, "count": 0, "children": children, "parent_path": None}
    recompute_node(node)
    return node


def parent_of(path):
    p = os.path.dirname(path)
    return p if (p and p != path) else None


# --------------------------------------------------------------------------- #
#  Views (slicing the in-memory tree -> small JSON payloads)
# --------------------------------------------------------------------------- #

def _shallow(n):
    return {"name": n["name"], "path": n["path"],
            "size": n["size"], "count": n["count"],
            "child_count": len(n.get("children", []) or [])}


def view_node(root, depth, max_children=300, max_nodes=8000):
    """Return a bounded-depth, child-capped copy of `root`.

    Expansion is BREADTH-FIRST: every node's direct children are added before
    any of them is expanded further.  This guarantees that shallow siblings
    (e.g. all 47 entries in your home dir) are never starved by one large
    sibling's deep subtree eating the whole node budget.

    Total nodes are capped by `max_nodes`; once hit, remaining nodes stay as
    leaves (no inner rings) but every slice keeps its true size, so proportions
    stay exact.  Nodes with more than `max_children` direct children aggregate
    the tail into a single "(other)" slice so each ring still sums to 100%.
    """
    import collections
    out_root = _shallow(root)
    if depth <= 0:
        return out_root
    queue = collections.deque()
    queue.append((root, out_root, depth))
    count = 1
    while queue and count < max_nodes:
        src, view, d = queue.popleft()
        src_children = sorted(src.get("children", []) or [],
                              key=lambda x: x["size"], reverse=True)
        if not src_children:
            continue
        overflow = 0
        if len(src_children) > max_children:
            shown = src_children[:max_children - 1]
            tail = src_children[max_children - 1:]
        else:
            shown = src_children
            tail = []
        kids = []
        deferred = []              # shown siblings that didn't fit the budget
        for c in shown:
            if count >= max_nodes:
                deferred.append(c)
                continue
            vc = _shallow(c)
            count += 1
            kids.append(vc)
            if d - 1 > 0:
                queue.append((c, vc, d - 1))
        agg = tail + deferred
        if agg:
            kids.append({
                "name": "(other)",
                "path": src["path"].rstrip("/") + "/__other__",
                "size": sum(c["size"] for c in agg),
                "count": sum(c["count"] for c in agg),
                "child_count": 0,
                "children": [],
            })
            count += 1
            view["overflow"] = len(agg)
        view["children"] = kids
    return out_root


def list_files(path):
    """Direct files in `path` with the small ones aggregated.

    Returns {"total": N, "files": [...]}.  If there are <= 20 files, all are
    returned (largest first).  Otherwise: list every file >= 100 MiB; if that's
    fewer than 10, top up with the largest <100 MiB files until 10 are shown
    (or we run out); fold everything else into a single "(others)" entry.
    """
    files = []
    try:
        it = os.scandir(path)
    except OSError:
        return {"total": 0, "files": []}
    for e in it:
        try:
            if e.is_file(follow_symlinks=False):
                files.append((e.name, _size_of(e.stat(follow_symlinks=False))))
        except OSError:
            continue
    total = len(files)
    files.sort(key=lambda x: x[1], reverse=True)
    MB100 = 100 * 1024 * 1024
    if total <= 20:
        return {"total": total,
                "files": [{"name": n, "size": s} for n, s in files]}
    big = [(n, s) for n, s in files if s >= MB100]          # already sorted desc
    small = [(n, s) for n, s in files if s < MB100]
    N = 10
    shown = big[:]
    i = 0
    while len(shown) < N and i < len(small):
        shown.append(small[i]); i += 1
    rest = small[i:]
    out = [{"name": n, "size": s} for n, s in shown]
    if rest:
        out.append({"name": "(others)", "size": sum(s for _, s in rest),
                    "count": len(rest), "is_other": True})
    return {"total": total, "files": out}


# --------------------------------------------------------------------------- #
#  Merged tree + cache (one fused on-disk index shared across all roots)
# --------------------------------------------------------------------------- #
#
# Every scanned subtree is merged into a single in-memory forest (NODES:
# path -> node).  Scanning an upper dir reuses already-scanned lower subtrees;
# re-scanning a lower dir updates its subtree and recomputes every scanned
# ancestor's totals upward.  Persisted as one file: ~/.cache/duw/tree.json.gz.


def merged_cache_path():
    cdir = os.path.expanduser("~/.cache/duw")
    try:
        os.makedirs(cdir, exist_ok=True)
    except OSError:
        return None
    return os.path.join(cdir, "tree.json.gz")


def strip_node(n):
    """Drop runtime-only fields for serialization."""
    return {"name": n["name"], "path": n["path"],
            "self_size": n["self_size"], "self_count": n["self_count"],
            "size": n["size"], "count": n["count"],
            "children": [strip_node(c) for c in n["children"]]}


def compute_roots():
    """Topmost nodes of the merged forest (parent absent from NODES)."""
    roots, seen = [], set()
    for n in NODES.values():
        pp = n.get("parent_path")
        if (not pp or pp not in NODES) and n["path"] not in seen:
            roots.append(n)
            seen.add(n["path"])
    return roots


def install_subtree(node, parent_path):
    """Install a freshly built subtree into NODES, setting parent links."""
    node["parent_path"] = parent_path
    NODES[node["path"]] = node
    for c in node["children"]:
        install_subtree(c, node["path"])


def install_loaded(node, parent_path):
    """Populate NODES from a deserialized nested tree; recompute bottom-up."""
    node["parent_path"] = parent_path
    for c in node["children"]:
        install_loaded(c, node["path"])
    recompute_node(node)
    NODES[node["path"]] = node


def recompute_up(path):
    """Recompute size/count for `path` and every ancestor present in NODES."""
    while path and path in NODES:
        n = NODES[path]
        recompute_node(n)
        n["children"].sort(key=lambda x: x["size"], reverse=True)
        path = n.get("parent_path")


def merge_subtree(new_root):
    """Merge a freshly built `new_root` into the forest and propagate totals up
    to all scanned ancestors."""
    parent_path = parent_of(new_root["path"])
    install_subtree(new_root, parent_path)
    if parent_path in NODES:
        parent = NODES[parent_path]
        for i, c in enumerate(parent["children"]):
            if c["path"] == new_root["path"]:
                parent["children"][i] = new_root
                break
        else:
            parent["children"].append(new_root)
        recompute_up(parent_path)


def migrate_node(old, parent_path):
    """Convert a legacy {size,count,children} node to the new self_* format."""
    children = [migrate_node(c, old["path"])
                for c in old.get("children", []) if isinstance(c, dict)]
    children.sort(key=lambda x: x["size"], reverse=True)
    self_size = old.get("size", 0) - sum(c["size"] for c in children)
    self_count = old.get("count", 0) - sum(c["count"] for c in children)
    return {"name": old.get("name"), "path": old["path"],
            "self_size": self_size, "self_count": self_count,
            "size": old.get("size", 0), "count": old.get("count", 0),
            "children": children, "parent_path": parent_path}


def migrate_old_caches():
    """One-time import of legacy per-root caches (~/.cache/duw/<hash>.json.gz)
    into the merged forest. Returns the number imported."""
    cdir = os.path.expanduser("~/.cache/duw")
    if not os.path.isdir(cdir):
        return 0
    n = 0
    for fn in sorted(os.listdir(cdir)):
        if not fn.endswith(".json.gz") or fn in ("tree.json.gz", "tree.json.gz.tmp"):
            continue
        try:
            with gzip.open(os.path.join(cdir, fn), "rt", encoding="utf-8") as f:
                old = json.load(f)
        except (OSError, ValueError):
            continue
        if not isinstance(old, dict) or "path" not in old:
            continue
        root = migrate_node(old, None)
        install_loaded(root, parent_of(root["path"]))
        n += 1
        try:
            os.remove(os.path.join(cdir, fn))
        except OSError:
            pass
    return n


def ensure_cache_loaded():
    global _CACHE_LOADED
    if _CACHE_LOADED:
        return
    _CACHE_LOADED = True
    cp = merged_cache_path()
    if cp and os.path.exists(cp):
        try:
            with gzip.open(cp, "rt", encoding="utf-8") as f:
                forest = json.load(f)
        except (OSError, ValueError) as exc:
            sys.stderr.write(f"duw: merged cache unreadable, starting fresh: {exc}\n")
            return
        for root in forest:
            install_loaded(root, parent_of(root["path"]))
        sys.stderr.write(f"duw: loaded merged cache: {len(NODES):,} nodes\n")
        return
    # no merged cache yet — try migrating legacy per-root caches
    n = migrate_old_caches()
    if n:
        sys.stderr.write(f"duw: migrated {n} legacy cache(s) into merged cache "
                         f"({len(NODES):,} nodes)\n")
        save_merged_cache()


def save_merged_cache():
    cp = merged_cache_path()
    if not cp:
        return
    forest = [strip_node(r) for r in compute_roots()]
    tmp = cp + ".tmp"
    try:
        with gzip.open(tmp, "wt", encoding="utf-8") as f:
            json.dump(forest, f)
        os.replace(tmp, cp)
    except OSError as exc:
        sys.stderr.write(f"duw: could not write merged cache: {exc}\n")


# --------------------------------------------------------------------------- #
#  Server state + background scanning
# --------------------------------------------------------------------------- #

NODES = {}               # path -> node  (the merged forest, flat index)
CURRENT_ROOT = None      # path the user is currently browsing
_CACHE_LOADED = False

STATE = {
    "status": "idle",     # idle | scanning | ready | error
    "root": None,
    "message": "",
    "dirs": 0,
    "files": 0,
    "size": 0,
    "elapsed": 0.0,
    "started_at": 0.0,
}
STATE_LOCK = threading.Lock()
SCAN_LOCK = threading.Lock()       # serialize scans
_SCANNING = False


def status_snapshot():
    with STATE_LOCK:
        snap = dict(STATE)
    if snap["status"] == "scanning":
        snap["elapsed"] = round(time.time() - snap["started_at"], 1)
        try:
            snap["dirs"] = int(_COUNTER.value) if _COUNTER is not None else snap["dirs"]
        except Exception:
            pass
    return snap


def _set_state(**kw):
    with STATE_LOCK:
        STATE.update(kw)


def run_scan(path, rebuild=False):
    """Scan (or reuse) `path` and merge into the shared forest, in background."""
    global CURRENT_ROOT, _COUNTER
    path = os.path.abspath(path)
    if not os.path.isdir(path):
        _set_state(status="error", root=path, message=f"not a directory: {path}")
        return
    if not SCAN_LOCK.acquire(blocking=False):
        return                      # a scan is already running
    try:
        ensure_cache_loaded()
        # Reuse: path already lives somewhere in the merged forest.
        if not rebuild and path in NODES:
            node = NODES[path]
            CURRENT_ROOT = path
            _set_state(status="ready", root=path, size=node["size"],
                       files=node["count"], dirs=_count_dirs(node),
                       elapsed=0.0, message="cached")
            sys.stderr.write(f"duw: reusing cached data for {path} "
                             f"({fmt_bytes(node['size'])})\n")
            return
        global_state_scanning(path)
        _COUNTER = mp.Value("Q", 0, lock=False)
        t0 = time.time()
        try:
            new_root = build_tree_reuse(path, force=rebuild)
        except Exception as exc:
            _set_state(status="error", root=path, message=str(exc))
            return
        finally:
            _COUNTER = None
        merge_subtree(new_root)
        CURRENT_ROOT = path
        ndirs = _count_dirs(new_root)
        dt = time.time() - t0
        save_merged_cache()
        _set_state(status="ready", root=path, size=new_root["size"],
                   files=new_root["count"], dirs=ndirs, elapsed=round(dt, 1),
                   message="ready")
        sys.stderr.write(
            f"duw: indexed {path}: {ndirs:,} dirs scanned, "
            f"{fmt_bytes(new_root['size'])} total in {dt:.1f}s "
            f"(merged forest: {len(NODES):,} nodes)\n")
    finally:
        SCAN_LOCK.release()


def global_state_scanning(path):
    _set_state(status="scanning", root=path, message="indexing",
               dirs=0, files=0, size=0, started_at=time.time(), elapsed=0.0)


def start_scan(path, rebuild=False):
    """Trigger a background scan unless one is already running."""
    global _SCANNING
    with STATE_LOCK:
        _SCANNING_FLAG = STATE["status"] == "scanning"
    if _SCANNING_FLAG:
        return False
    t = threading.Thread(target=run_scan, args=(path, rebuild), daemon=True)
    t.start()
    return True


# --------------------------------------------------------------------------- #
#  HTTP
# --------------------------------------------------------------------------- #

PAGE = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>duw — disk usage</title>
<style>
  :root { color-scheme: light dark; }
  * { box-sizing: border-box; }
  html,body { height:100%; margin:0; }
  body {
    font: 14px/1.45 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
    background:#0f1115; color:#e6e6e6; display:flex; flex-direction:column;
  }
  header {
    padding:8px 14px; background:#161a22; border-bottom:1px solid #262b36;
    display:flex; align-items:center; gap:10px; flex-wrap:wrap;
  }
  header .brand { font-weight:600; letter-spacing:.5px; color:#7fd1ff; cursor:pointer; }
  #crumbs { display:flex; gap:2px; flex-wrap:wrap; align-items:center;
            font:12px ui-monospace,SFMono-Regular,Menlo,monospace; color:#9aa; }
  #crumbs a { color:#9bc; text-decoration:none; padding:2px 4px; border-radius:4px; }
  #crumbs a:hover { background:#222a36; color:#cdf; }
  #crumbs .cur { color:#fff; }
  #crumbs .sep { color:#445; }
  header .stats { margin-left:auto; color:#8a93a6; font-size:12px; }
  header .stats b { color:#7fd1ff; }
  main { flex:1; display:flex; min-height:0; }
  #app { flex:1; display:flex; min-height:0; }
  #chart-wrap { flex:1 1 60%; position:relative; display:flex;
                align-items:center; justify-content:center; min-width:0; }
  svg { width:100%; height:100%; max-height:100%; display:block; }
  #tip {
    position:absolute; pointer-events:none; background:#000c;
    border:1px solid #334; border-radius:6px; padding:6px 9px; font-size:12px;
    color:#eee; display:none; max-width:280px; word-break:break-all;
  }
  #side { flex:0 0 38%; min-width:320px; max-width:560px;
          border-left:1px solid #262b36; background:#11141b;
          display:flex; flex-direction:column; min-height:0;
          overflow:auto; }            /* whole panel scrolls together */
  .sec { padding:10px 14px 6px; border-bottom:1px solid #1d222c; }
  .sec h3 { margin:0 0 6px; font-size:11px; text-transform:uppercase;
            letter-spacing:1px; color:#6c7588; font-weight:600; }
  .row { display:flex; align-items:center; gap:8px; padding:3px 0;
         cursor:pointer; border-radius:4px; }
  .row:hover { background:#1a212c; }
  .sw { width:10px; height:10px; border-radius:2px; flex:0 0 auto; }
  .rname { flex:1; min-width:0; overflow:hidden; text-overflow:ellipsis;
           white-space:nowrap; font-family:ui-monospace,monospace; font-size:12px; }
  .rbar { height:7px; border-radius:3px; background:#3a4458; position:relative; flex:1 1 20%; }
  .rbar > i { display:block; height:100%; border-radius:3px; }
  .rmeta { font-size:11px; color:#7c8699; width:104px; text-align:right;
           font-family:ui-monospace,monospace; }
  .foot { padding:8px 14px; font-size:11px; color:#5b6477; }
  .empty { color:#5b6477; font-style:italic; padding:4px 0; }
  a.link { color:#7fd1ff; cursor:pointer; text-decoration:none; }
  a.link:hover { text-decoration:underline; }

  /* header path bar + icons */
  #pathbar { flex:1; min-width:120px; display:flex; align-items:center; gap:2px;
             overflow:hidden; }
  #crumbs { display:flex; gap:0; flex-wrap:wrap; align-items:center;
            font:12px ui-monospace,SFMono-Regular,Menlo,monospace; color:#9aa;
            overflow:hidden; }
  #crumbs a { color:#9bc; text-decoration:none; padding:2px 3px; border-radius:4px;
              cursor:pointer; }
  #crumbs a:hover { background:#222a36; color:#cdf; }
  #crumbs .cur { color:#fff; cursor:default; }
  #crumbs .sep { color:#445; padding:0 1px; }
  #pathedit {
    flex:1; min-width:80px; padding:4px 8px; border-radius:6px;
    border:1px solid #2c333f; background:#161a22; color:#e6e6e6;
    font:12px ui-monospace,monospace;
  }
  #pathedit:focus { outline:none; border-color:#3b6e8f; }
  .icon {
    flex:0 0 auto; width:30px; height:30px; display:flex; align-items:center;
    justify-content:center; border-radius:6px; border:1px solid #2c333f;
    background:#1c2530; color:#cde; cursor:pointer; font-size:16px; line-height:1;
  }
  .icon:hover { background:#243140; }

  /* main message pane: loading / not-scanned / scanning / error */
  #msgpane { flex:1; display:flex; align-items:center; justify-content:center;
             flex-direction:column; gap:14px; text-align:center; color:#9aa;
             padding:24px; }
  #msgpane h2 { margin:0; color:#cde; font-weight:600; font-size:18px; }
  #msgpane .path { font:13px ui-monospace,monospace; color:#7fd1ff; word-break:break-all; }
  #msgpane .sub { color:#7c8699; font-size:13px; max-width:520px; }
  .btn {
    padding:9px 18px; border-radius:8px; border:1px solid #2c333f;
    background:#1c2530; color:#cde; cursor:pointer; font-size:14px;
  }
  .btn:hover { background:#243140; }
  .btn.primary { background:#1f6f9c; border-color:#2a86b8; color:#fff; }
  .btn.primary:hover { background:#2580b0; }
  .spinner {
    width:34px; height:34px; border-radius:50%; border:3px solid #262b36;
    border-top-color:#7fd1ff; animation:spin 0.8s linear infinite;
  }
  @keyframes spin { to { transform:rotate(360deg); } }
  #msgpane .nums { font:12px ui-monospace,monospace; color:#8a93a6; }
  #msgpane .nums b { color:#7fd1ff; }
  #msgpane .err { color:#ff8b8b; max-width:560px; word-break:break-all; }
  .hint { color:#5b6477; font-size:11px; }

  /* right-click context menu + toast */
  #ctxmenu {
    position:fixed; z-index:50; display:none; min-width:150px;
    background:#161a22; border:1px solid #2c333f; border-radius:8px;
    box-shadow:0 6px 20px #0008; padding:4px;
  }
  #ctxmenu button {
    display:block; width:100%; text-align:left; padding:7px 12px; border:none;
    background:none; color:#cde; cursor:pointer; border-radius:5px;
    font:13px -apple-system,Segoe UI,Roboto,sans-serif;
  }
  #ctxmenu button:hover { background:#222a36; }
  #toast {
    position:fixed; left:50%; bottom:28px; transform:translateX(-50%);
    z-index:60; background:#1f6f9c; color:#fff; padding:7px 14px; border-radius:8px;
    font-size:13px; box-shadow:0 4px 14px #0008; opacity:0; transition:opacity .15s;
    pointer-events:none; max-width:80vw; overflow:hidden; text-overflow:ellipsis;
    white-space:nowrap;
  }
  #toast.show { opacity:1; }
</style>
</head><body>
<header>
  <div class="brand" id="brand" title="go to $HOME">duw</div>
  <div id="pathbar">
    <nav id="crumbs"></nav>
    <input id="pathedit" style="display:none" spellcheck="false" autocomplete="off"/>
  </div>
  <div class="stats" id="stats"></div>
  <button class="icon" id="editbtn" title="edit path (type a path to jump)" type="button">&#x270E;</button>
  <button class="icon" id="refreshbtn" title="re-analyze this directory now" type="button">&#x21BB;</button>
</header>
<main>
  <div id="app" style="display:none;">
    <div id="chart-wrap">
      <svg id="chart" viewBox="0 0 800 800" preserveAspectRatio="xMidYMid meet"></svg>
      <div id="tip"></div>
    </div>
    <aside id="side">
      <div class="sec">
        <h3>Subdirectories <span id="dircount"></span></h3>
        <div id="children"></div>
      </div>
      <div class="sec">
        <h3>Files <span id="filecount"></span></h3>
        <div id="files"></div>
      </div>
      <div class="foot">click a slice or row to dive in · click the centre to go up · <a class="link" id="uplink">↑ up</a></div>
    </aside>
  </div>
  <div id="msgpane" style="display:none;"></div>
</main>
<div id="ctxmenu"><button id="ctxcopy" type="button">Copy path</button></div>
<div id="toast"></div>
<script>
const SVGNS = "http://www.w3.org/2000/svg";
const MAX_DEPTH = 6;
const FETCH_DEPTH = 6;
const CX = 400, CY = 400, RMAX = 380;
let HOME = "/", currentPath = null, curNode = null;
let scanPath = null, scanError = null, pollTimer = null;

function fmtSize(b){
  if(b==null||isNaN(b)) return "—";
  const u=["B","KiB","MiB","GiB","TiB","PiB"]; let i=0; let v=b;
  while(v>=1024 && i<u.length-1){ v/=1024; i++; }
  return (v>=100? v.toFixed(0) : v>=10? v.toFixed(1) : v.toFixed(2)) + " " + u[i];
}
function fmtCount(n){ return (n||0).toLocaleString(); }
function pct(part, whole){ return whole>0 ? (part/whole*100) : 0; }
function colour(name){
  let h=0; for(let i=0;i<name.length;i++){ h=(h*31+name.charCodeAt(i))>>>0; }
  return `hsl(${h%360} ${58+ (h>>3)%22}% ${42+ (h>>7)%14}%)`;
}
function htmlEsc(s){ return s.replace(/[&<>]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;"}[c])); }

function arcPath(r0,r1,t0,t1){
  if(t1-t0>=1){
    return `M ${CX-r1} ${CY} A ${r1} ${r1} 0 1 0 ${CX+r1} ${CY} A ${r1} ${r1} 0 1 0 ${CX-r1} ${CY} `
         + `M ${CX-r0} ${CY} A ${r0} ${r0} 0 1 1 ${CX+r0} ${CY} A ${r0} ${r0} 0 1 1 ${CX-r0} ${CY} Z`;
  }
  const a0=t0*2*Math.PI - Math.PI/2, a1=t1*2*Math.PI - Math.PI/2;
  const large=(t1-t0)>0.5?1:0;
  const x0=CX+r1*Math.cos(a0), y0=CY+r1*Math.sin(a0);
  const x1=CX+r1*Math.cos(a1), y1=CY+r1*Math.sin(a1);
  const x2=CX+r0*Math.cos(a1), y2=CY+r0*Math.sin(a1);
  const x3=CX+r0*Math.cos(a0), y3=CY+r0*Math.sin(a0);
  return `M ${x0} ${y0} A ${r1} ${r1} 0 ${large} 1 ${x1} ${y1} L ${x2} ${y2} A ${r0} ${r0} 0 ${large} 0 ${x3} ${y3} Z`;
}

async function api(path, qs){
  const u = path + (qs? ("?"+new URLSearchParams(qs)) : "");
  const r = await fetch(u);
  if(!r.ok){ const e = await r.json().catch(()=>({})); throw new Error(e.error||("HTTP "+r.status)); }
  return r.json();
}

function el(tag, attrs, kids){
  const e=document.createElementNS(SVGNS,tag);
  if(attrs) for(const k in attrs) e.setAttribute(k, attrs[k]);
  if(kids) for(const k of kids) e.appendChild(k);
  return e;
}

// ---- sunburst ----
function layout(node, t0, t1, depth){
  node._t0=t0; node._t1=t1; node._d=depth;
  const ch=node.children||[];
  const tot=ch.reduce((s,c)=>s+(c.size||0),0);
  if(tot<=0) return;
  let acc=t0; const span=t1-t0;
  for(const c of ch){
    const f=c.size/tot;
    layout(c, acc, acc+span*f, depth+1);
    acc+=span*f;
  }
}
function renderSunburst(){
  const svg=document.getElementById("chart");
  svg.innerHTML="";
  if(!curNode) return;
  layout(curNode, 0, 1, 0);
  const gAll=el("g",{}); svg.appendChild(gAll);
  const items=[];
  (function walk(n){
    if(!n.children) return;
    for(const c of n.children){
      if(c._d<=MAX_DEPTH && (c._t1-c._t0)>0.0005) items.push(c);
      walk(c);
    }
  })(curNode);
  const step=RMAX/(MAX_DEPTH+1);
  for(const n of items){
    const other = n.name==="(other)";
    const p=el("path",{
      d: arcPath(n._d*step,(n._d+1)*step,n._t0,n._t1),
      fill: other? "#3a4458" : colour(n.name), stroke:"#0f1115", "stroke-width":1,
    });
    p.style.cursor = other? "default" : "pointer";
    if(!other){ p.addEventListener("click", ()=> dive(n.path));
                p.addEventListener("contextmenu", e=>ctxMenuFor(e, n.path)); }
    p.addEventListener("mouseenter", (e)=> showTip(e,n));
    p.addEventListener("mousemove", moveTip);
    p.addEventListener("mouseleave", hideTip);
    gAll.appendChild(p);
  }
  const lab=el("text",{x:CX,y:CY-6,"text-anchor":"middle","font-size":15,fill:"#fff","font-weight":600});
  lab.textContent=curNode.name.length>22? curNode.name.slice(0,20)+"…" : curNode.name;
  const lab2=el("text",{x:CX,y:CY+14,"text-anchor":"middle","font-size":13,fill:"#9bc"});
  lab2.textContent=fmtSize(curNode.size);
  gAll.appendChild(lab); gAll.appendChild(lab2);
  const up=el("circle",{cx:CX,cy:CY,r:step*0.92,fill:"transparent",style:"cursor:pointer"});
  up.addEventListener("click", goUp);
  gAll.appendChild(up);
}
function renderSide(){
  const st=document.getElementById("stats");
  st.innerHTML = `<b>${fmtSize(curNode.size)}</b> · ${fmtCount(curNode.count)} files`;
  const chBox=document.getElementById("children");
  const all = curNode.children || [];
  const total = curNode.child_count != null ? curNode.child_count : all.length;
  document.getElementById("dircount").textContent = `(${fmtCount(total)})`;
  chBox.innerHTML="";
  if(!all.length){ chBox.innerHTML='<div class="empty">no subdirectories</div>'; return; }

  // <=10 subdirs: show all.  >10: keep those >= 1 GiB, top up with the largest
  // <1 GiB ones until 10 are shown, fold the rest into (others).
  const ONE_GB = 1024*1024*1024;
  const real = all.filter(c=>c.name!=="(other)").slice().sort((a,b)=>b.size-a.size);
  const preOther = all.find(c=>c.name==="(other)");
  let rows;
  if (real.length <= 10) {
    rows = real.slice();
    if (preOther) rows.push({name:"(others)", size:preOther.size,
                             count:curNode.overflow||0, is_other:true});
  } else {
    const big = real.filter(c=>c.size >= ONE_GB);
    const small = real.filter(c=>c.size < ONE_GB);
    const N = 10;
    const shown = big.slice();
    let i = 0;
    while (shown.length < N && i < small.length) { shown.push(small[i]); i++; }
    const rest = small.slice(i);
    let oSize = rest.reduce((s,c)=>s+c.size, 0);
    let oCount = rest.length;
    if (preOther) { oSize += preOther.size; oCount += (curNode.overflow||0); }
    rows = shown;
    if (oCount > 0) rows.push({name:"(others)", size:oSize, count:oCount, is_other:true});
  }

  const denom = curNode.size || 1;
  for(const c of rows){
    const other = !!c.is_other;
    const row=document.createElement("div"); row.className="row";
    if(!other){ row.addEventListener("click",()=>dive(c.path));
                row.addEventListener("contextmenu", e=>ctxMenuFor(e, c.path)); }
    else row.style.cursor="default";
    const sw=document.createElement("span"); sw.className="sw";
      sw.style.background = other ? "#6b7689" : colour(c.name);
    const nm=document.createElement("span"); nm.className="rname";
      nm.textContent=c.name; nm.title = other ? "aggregated smaller directories" : (c.path||c.name);
    const bar=document.createElement("div"); bar.className="rbar";
    const i=document.createElement("i"); i.style.width=(c.size/denom*100)+"%";
      i.style.background = other ? "#6b7689" : colour(c.name); bar.appendChild(i);
    const meta=document.createElement("span"); meta.className="rmeta";
    const pp = pct(c.size,curNode.size).toFixed(1)+"%";
    if (other) meta.innerHTML = `${fmtSize(c.size)}<br/>${pp} · ${fmtCount(c.count)} dirs`;
    else meta.innerHTML = `${fmtSize(c.size)}<br/>${pp}`;
    row.append(sw,nm,bar,meta); chBox.appendChild(row);
  }
}
async function renderFiles(){
  const box=document.getElementById("files");
  box.innerHTML='<div class="empty">loading…</div>';
  try{
    const data=await api("/api/files",{path:currentPath});
    document.getElementById("filecount").textContent=`(${fmtCount(data.total)})`;
    box.innerHTML="";
    if(!data.files.length){ box.innerHTML='<div class="empty">no files here</div>'; return; }
    const denom = curNode.size || 1;
    for(const f of data.files){
      const other = !!f.is_other;
      const row=document.createElement("div"); row.className="row";
      row.style.cursor="default";
      if(!other){ const fp=joinPath(currentPath, f.name);
                  row.addEventListener("contextmenu", e=>ctxMenuFor(e, fp)); }
      const sw=document.createElement("span"); sw.className="sw";
        sw.style.background = other ? "#a9b2c4" : "#4b5568";
      const nm=document.createElement("span"); nm.className="rname";
        nm.textContent=f.name; nm.title = other ? "aggregated smaller files" : f.name;
      const bar=document.createElement("div"); bar.className="rbar";
      const i=document.createElement("i"); i.style.width=(f.size/denom*100)+"%";
        i.style.background = other ? "#a9b2c4" : "#6b7689"; bar.appendChild(i);
      const meta=document.createElement("span"); meta.className="rmeta";
      const pp = pct(f.size,curNode.size).toFixed(1)+"%";
      if (other) meta.innerHTML=`${fmtSize(f.size)}<br/>${pp} · ${fmtCount(f.count)} files`;
      else meta.innerHTML=`${fmtSize(f.size)}<br/>${pp}`;
      row.append(sw,nm,bar,meta); box.appendChild(row);
    }
  }catch(e){ box.innerHTML='<div class="empty">—</div>'; }
}
function renderCrumbs(){
  const cb=document.getElementById("crumbs"); cb.innerHTML="";
  if(!currentPath){ return; }
  if(currentPath==="/"){
    const a=document.createElement("a"); a.textContent="/"; a.classList.add("cur"); cb.appendChild(a); return;
  }
  const parts=currentPath.split("/").filter(Boolean);
  let acc="";
  parts.forEach((p,i)=>{
    acc = acc + "/" + p;
    const segPath = acc;            // per-iteration binding for the closure
    if(i>0){ const sp=document.createElement("span"); sp.className="sep"; sp.textContent="/"; cb.appendChild(sp); }
    const a=document.createElement("a");
    a.textContent = (i===0 ? "/"+p : p);
    if(i===parts.length-1) a.classList.add("cur");
    else a.addEventListener("click", ()=>jump(segPath));
    cb.appendChild(a);
  });
}
async function jump(path){
  if(!path || path===currentPath) return;
  currentPath=path;
  await loadCurrent();
}
async function loadCurrent(){
  renderCrumbs();
  document.getElementById("stats").innerHTML="";
  showMsg("loading");
  try{
    const node=await api("/api/subtree",{path:currentPath, depth:FETCH_DEPTH});
    curNode=node;
    showApp();
  }catch(e){
    curNode=null;
    showMsg("notscanned");
  }
}
async function dive(path){
  await jump(path);
}
function parentPath(p){
  if(!p || p==="/") return null;
  const s=p.replace(/\/+$/,"");
  const i=s.lastIndexOf("/");
  if(i<0) return null;
  if(i===0) return "/";                 // /foo -> /
  return s.slice(0,i);
}
function goUp(){
  if(!currentPath) return;
  const parent=parentPath(currentPath);
  if(!parent || parent===currentPath) return;
  dive(parent);   // stays put if the parent isn't scanned
}

// tooltip
const tipEl=document.getElementById("tip");
function showTip(e,n){
  tipEl.style.display="block";
  tipEl.innerHTML = `<b>${htmlEsc(n.name)}</b><br/>${fmtSize(n.size)}`
    + ` · ${pct(n.size,curNode.size).toFixed(1)}% of ${htmlEsc(curNode.name)}`
    + (n.count!=null? `<br/>${fmtCount(n.count)} files`:"");
  moveTip(e);
}
function moveTip(e){
  const wrap=document.getElementById("chart-wrap").getBoundingClientRect();
  tipEl.style.left=Math.min(e.clientX-wrap.left+12, wrap.width-260)+"px";
  tipEl.style.top=(e.clientY-wrap.top+12)+"px";
}
function hideTip(){ tipEl.style.display="none"; }

// ---- right-click "copy path" menu ----
function joinPath(base, name){
  if(!base || base==="/") return "/"+name;
  return base.endsWith("/") ? base+name : base+"/"+name;
}
let ctxPath=null;
const ctxMenu=document.getElementById("ctxmenu");
const toastEl=document.getElementById("toast");
let toastTimer=null;
function showCtxMenu(x,y,path){
  ctxPath=path;
  ctxMenu.style.display="block";
  const w=ctxMenu.offsetWidth||150, h=ctxMenu.offsetHeight||40;
  ctxMenu.style.left=Math.min(x, window.innerWidth-w-6)+"px";
  ctxMenu.style.top=Math.min(y, window.innerHeight-h-6)+"px";
}
function hideCtxMenu(){ ctxMenu.style.display="none"; ctxPath=null; }
function toast(msg){
  toastEl.textContent=msg; toastEl.classList.add("show");
  if(toastTimer) clearTimeout(toastTimer);
  toastTimer=setTimeout(()=>toastEl.classList.remove("show"), 1400);
}
function fallbackCopy(p){
  const ta=document.createElement("textarea");
  ta.value=p; ta.style.position="fixed"; ta.style.top="-1000px";
  document.body.appendChild(ta); ta.focus(); ta.select();
  try{ document.execCommand("copy"); toast("copied: "+p); }catch(e){ toast("copy failed"); }
  document.body.removeChild(ta);
}
function copyPath(){
  const p=ctxPath||""; hideCtxMenu();
  if(!p) return;
  if(navigator.clipboard && navigator.clipboard.writeText){
    navigator.clipboard.writeText(p).then(()=>toast("copied: "+p)).catch(()=>fallbackCopy(p));
  } else fallbackCopy(p);
}
function ctxMenuFor(e, path){
  e.preventDefault(); e.stopPropagation();
  showCtxMenu(e.clientX, e.clientY, path);
}
ctxMenu.addEventListener("click", e=>e.stopPropagation());
document.getElementById("ctxcopy").addEventListener("click", copyPath);
document.addEventListener("click", hideCtxMenu);
document.addEventListener("contextmenu", hideCtxMenu);
document.addEventListener("scroll", hideCtxMenu, true);

// ---- main pane switching ----
function showApp(){
  document.getElementById("msgpane").style.display="none";
  document.getElementById("app").style.display="flex";
  renderSunburst(); renderSide(); renderFiles();
}
function showMsg(kind){
  document.getElementById("app").style.display="none";
  document.getElementById("stats").innerHTML="";
  const m=document.getElementById("msgpane");
  m.style.display="flex";
  if(kind==="loading"){
    m.innerHTML='<div class="sub">loading…</div>';
  } else if(kind==="notscanned"){
    m.innerHTML = `<h2>Not analyzed yet</h2>`
      + `<div class="path">${htmlEsc(currentPath||"")}</div>`
      + `<div class="sub">This directory hasn't been scanned. The first scan runs once in parallel; afterward browsing here is instant (and it merges into the shared cache).</div>`
      + `<button class="btn primary" id="analyzenow">Analyze now</button>`;
    document.getElementById("analyzenow").addEventListener("click", ()=>analyzeCurrent(false));
  } else if(kind==="scanning"){
    m.innerHTML = `<div class="spinner"></div>`
      + `<div class="sub">indexing <span class="path">${htmlEsc(scanPath||"")}</span> …</div>`
      + `<div class="nums"></div>`;
  } else if(kind==="error"){
    m.innerHTML = `<h2 style="color:#ff8b8b">Analysis failed</h2>`
      + `<div class="err">${htmlEsc(scanError||"error")}</div>`
      + `<button class="btn" id="errback">Back</button>`;
    document.getElementById("errback").addEventListener("click", ()=>loadCurrent());
  }
}

async function analyzeCurrent(rebuild){
  if(!currentPath) return;
  scanPath=currentPath; scanError=null;
  showMsg("scanning");
  try{ await api("/api/scan",{path:currentPath, rebuild:rebuild?1:0}); }
  catch(e){ /* server may reject if a scan is already running; polling reflects it */ }
  pollScan();
}
function pollScan(){
  if(pollTimer) clearInterval(pollTimer);
  const tick=async()=>{
    let s;
    try{ s=await api("/api/status"); }catch(e){ return; }
    if(s.status==="scanning"){
      const n=document.querySelector("#msgpane .nums");
      if(n) n.innerHTML=`<b>${fmtCount(s.dirs)}</b> dirs scanned · <b>${s.elapsed.toFixed(1)}s</b>`;
    } else if(s.status==="ready"){
      if(pollTimer){ clearInterval(pollTimer); pollTimer=null; }
      await loadCurrent();   // re-fetch current path: results, or not-scanned
    } else if(s.status==="error"){
      if(pollTimer){ clearInterval(pollTimer); pollTimer=null; }
      scanError=s.message||"error";
      showMsg("error");
    } else { // idle (scan didn't start / finished elsewhere)
      if(pollTimer){ clearInterval(pollTimer); pollTimer=null; }
      await loadCurrent();
    }
  };
  tick();
  pollTimer=setInterval(tick,600);
}

// ---- path editing ----
function enterEdit(){
  const crumbs=document.getElementById("crumbs");
  const inp=document.getElementById("pathedit");
  crumbs.style.display="none";
  inp.style.display="block";
  inp.value=currentPath||"";
  inp.focus(); inp.select();
}
function exitEdit(){
  document.getElementById("pathedit").style.display="none";
  document.getElementById("crumbs").style.display="flex";
}

// ---- handlers ----
document.getElementById("editbtn").addEventListener("click", enterEdit);
document.getElementById("refreshbtn").addEventListener("click", ()=>analyzeCurrent(true));
document.getElementById("brand").addEventListener("click", ()=>jump(HOME));
document.getElementById("uplink").addEventListener("click", goUp);
document.getElementById("pathedit").addEventListener("keydown", e=>{
  if(e.key==="Enter"){
    e.preventDefault();
    const p=e.target.value.trim();
    exitEdit();
    if(p) jump(p);
  } else if(e.key==="Escape"){
    exitEdit();
  }
});
document.getElementById("pathedit").addEventListener("blur", exitEdit);
document.addEventListener("keydown", e=>{
  const editing=document.getElementById("pathedit").style.display!=="none";
  if(e.key==="Escape"){
    if(ctxMenu.style.display!=="none"){ hideCtxMenu(); return; }
    if(!editing) goUp();
  }
  if(e.key==="Backspace" && !editing && e.target===document.body) goUp();
});

(async function init(){
  try{ const sug=await api("/api/suggest"); if(sug&&sug.path) HOME=sug.path; }catch(e){}
  document.getElementById("brand").title="go to "+HOME;
  currentPath=HOME;
  let st=null;
  try{ st=await api("/api/status"); }catch(e){}
  if(st && st.status==="scanning"){ scanPath=st.root; showMsg("scanning"); pollScan(); return; }
  await loadCurrent();
})();
</script>
</body></html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, body, ctype, code=200):
        b = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(b)

    def _json(self, obj, code=200):
        self._send(json.dumps(obj), "application/json", code)

    def do_GET(self):
        parsed = urlparse(self.path)
        q = parse_qs(parsed.query)
        path = parsed.path

        if path in ("/", "/index.html"):
            self._send(PAGE, "text/html; charset=utf-8")
            return

        if path == "/api/status":
            self._json(status_snapshot())
            return

        if path == "/api/suggest":
            sug = os.path.expanduser("~")
            self._json({"path": sug})
            return

        if path == "/api/scan":
            p = (q.get("path") or [""])[0]
            rebuild = (q.get("rebuild") or ["0"])[0] == "1"
            if not p or not os.path.isdir(p):
                self._json({"ok": False, "error": "not a directory"}, 400)
                return
            started = start_scan(p, rebuild=rebuild)
            self._json({"ok": started, "started": started})
            return

        # endpoints below use the merged forest
        if path == "/api/root":
            n = NODES.get(CURRENT_ROOT) if CURRENT_ROOT else None
            self._json({"path": CURRENT_ROOT,
                        "size": n["size"] if n else 0,
                        "count": n["count"] if n else 0})
            return

        if path == "/api/subtree":
            p = (q.get("path") or [CURRENT_ROOT or ""])[0]
            depth = int((q.get("depth") or ["6"])[0])
            node = NODES.get(p)
            if node is None:
                self._json({"error": f"not scanned: {p}"}, 404)
                return
            self._json(view_node(node, depth))
            return

        if path == "/api/files":
            p = (q.get("path") or [CURRENT_ROOT or ""])[0]
            if not p or not os.path.isdir(p):
                self._json({"total": 0, "files": []})
                return
            self._json(list_files(p))
            return

        self._send("not found", "text/plain", 404)


def main():
    ap = argparse.ArgumentParser(description="duw — disk usage, web")
    ap.add_argument("root", nargs="?", default=None,
                    help="directory to analyze at startup (optional; "
                         "if omitted, pick it from the web page)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--db", help="cache file path override")
    ap.add_argument("--rebuild", action="store_true",
                    help="force a fresh index of the given root")
    args = ap.parse_args()

    if args.root and not os.path.isdir(args.root):
        sys.stderr.write(f"duw: not a directory: {args.root}\n")
        sys.exit(2)

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    url = f"http://{args.host}:{args.port}/"
    # Load the merged cache up front so the page can show $HOME instantly
    # (without waiting for an explicit Analyze click).
    ensure_cache_loaded()
    sys.stderr.write(f"duw: serving {url}  (open in browser; shows $HOME by default)\n")
    if args.root:
        sys.stderr.write(f"duw: queuing scan of {args.root} "
                         f"({'rebuild' if args.rebuild else 'auto'})\n")
        start_scan(args.root, rebuild=args.rebuild)
    sys.stderr.write("duw: Ctrl-C to stop\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        sys.stderr.write("\nduw: bye\n")


def _count_dirs(node):
    n = 0
    for c in node.get("children", []):
        n += 1 + _count_dirs(c)
    return n


def fmt_bytes(b):
    u = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    v = float(b)
    i = 0
    while v >= 1024 and i < len(u) - 1:
        v /= 1024
        i += 1
    return f"{v:.1f} {u[i]}"


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)
    main()
