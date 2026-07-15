# Per-RGW Dedup Thread Enable/Disable Configuration

## Overview

This change adds configuration options to control whether a given RGW instance
participates in dedup background processing. The mechanism follows the
established pattern used by Lifecycle (LC), Garbage Collection (GC), quota, and
restore threads.

## Motivation

In multi-RGW deployments, operators may want to dedicate certain RGW instances
to serving S3 requests while offloading background maintenance work (GC, LC,
dedup, etc.) to other instances. Ceph already provides `rgw_enable_gc_threads`
and `rgw_enable_lc_threads` for this purpose. Until now, the dedup background
thread lacked an equivalent toggle and would always start on every RGW instance.

## New Configuration Options

### `rgw_enable_dedup_threads`

| Property  | Value |
|-----------|-------|
| Type      | bool  |
| Default   | true  |
| Level     | advanced |
| Service   | rgw  |

Master switch that enables or disables the dedup background thread on this RGW
instance. When set to `false`, the dedup `Background` object is not created,
the background thread is not spawned, and this RGW does not register a RADOS
watch on the dedup control object.

At least one RGW per zone should have this option enabled for dedup operations
(estimate/exec) to function correctly.

### `rgw_nfs_run_dedup_threads`

| Property  | Value |
|-----------|-------|
| Type      | bool  |
| Default   | false |
| Level     | advanced |
| Service   | rgw  |

NFS override. When an RGW runs as an NFS-Ganesha gateway (librgw), this option
must also be set to `true` for the dedup thread to start. This mirrors the
behavior of `rgw_nfs_run_gc_threads` and `rgw_nfs_run_lc_threads`, which
default to `false` for NFS gateways.

## How It Works

### Decision Logic

The boolean logic is identical to GC and LC:

```
run_dedup = rgw_enable_dedup_threads AND (NOT nfs OR rgw_nfs_run_dedup_threads)
```

- If `rgw_enable_dedup_threads` is `false`, the thread does not run (regardless
  of NFS mode).
- If `rgw_enable_dedup_threads` is `true` and this is **not** an NFS gateway,
  the thread runs.
- If `rgw_enable_dedup_threads` is `true` and this **is** an NFS gateway, the
  thread runs only if `rgw_nfs_run_dedup_threads` is also `true`.

### Initialization Flow

The check is performed in `rgw::AppMain::init_dedup()` during RGW startup.
When the computed `run_dedup` value is `false`, `init_dedup()` returns
immediately without creating the `dedup_background` object. All existing code
that references `dedup_background` already null-checks the pointer (e.g.,
`if (dedup_background) { ... }`), so no additional changes are required in
shutdown or realm-reload paths.

### Interaction with Dedup Cluster Coordination

The dedup subsystem uses RADOS watch/notify on a shared control object
(`DEDUP_WATCH_OBJ`) for cross-RGW coordination (exec, pause, resume, abort).
An RGW with `rgw_enable_dedup_threads = false`:

- **Does not call `watch2()`** on the control object, so it is not a watcher.
- **Does not receive notifications**, so no ack is expected from it.
- **Does not grab shard tokens**, so it does not participate in work
  distribution.

This means disabled RGW instances are completely invisible to the dedup
coordination mechanism. They cause no timeouts, no hangs, and no missed
work. The remaining enabled RGW instances distribute all shard tokens among
themselves as usual.

### Runtime Control

This configuration is evaluated at startup only (same as `rgw_enable_gc_threads`
and `rgw_enable_lc_threads`). Changing it at runtime via `ceph config set`
requires an RGW restart to take effect.

For live control of an already-running dedup thread, use the existing admin
commands:

```
radosgw-admin dedup pause
radosgw-admin dedup resume
radosgw-admin dedup abort
```

## Usage Examples

Disable dedup on a specific RGW instance via the centralized config store:

```bash
ceph config set client.rgw.host2 rgw_enable_dedup_threads false
```

Or via `ceph.conf`:

```ini
[client.rgw.host2]
rgw_enable_dedup_threads = false
```

Enable dedup on an NFS gateway:

```bash
ceph config set client.rgw.nfs1 rgw_nfs_run_dedup_threads true
```

## Code Changes

1. **`src/common/options/rgw.yaml.in`** -- Added `rgw_enable_dedup_threads`
   (after `rgw_enable_restore_threads`) and `rgw_nfs_run_dedup_threads` (after
   `rgw_nfs_run_restore_threads`).

2. **`src/rgw/rgw_appmain.cc`** -- Added config evaluation at the top of
   `init_dedup()` to compute `run_dedup` and return early when `false`.
