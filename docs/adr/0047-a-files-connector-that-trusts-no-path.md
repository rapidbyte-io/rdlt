# ADR 0047: A files connector that trusts no path

Status: accepted, 2026-10-01.

## Context

The files source and destination (ADR 0006) and the position keeper of the log and change
sources (ADR 0036) are reference code that connector authors copy, and the files connectors ship
as a binary. They built paths by joining names: table names from the host, file paths from
manifests, entries of the directory tree. Whoever could create an entry under a root, or change a
manifest, could make the connector read, write or delete elsewhere, wait on a pipe, or read a
file of any size into memory; a reader of a lock file could hold the lock for good; and a
session kept every file a later commit superseded. ADR 0037 asks that persistent state is checked
when it is read and never written in a form its reader refuses.

## Decision

- **Everything is reached by name beneath an open directory.** The module `rooted` opens a
  directory once and reaches what is beneath it one path component at a time, each `openat` with
  `O_NOFOLLOW`: no name is joined into a path the kernel resolves, so no `..`, absolute path,
  link or rename leads out of the directory. A name is one normal component of at most 255
  bytes. Creating, linking, renaming and removing take a name in an open directory too; removing
  a tree unlinks a link and never enters it. The configured root itself is opened as the operator
  wrote it.
- **`rustix`, not `cap-std` or `openat2`.** `cap-std` confines a path to its root but follows
  links that stay inside, so the no-link rule would still be a walk written on top of it, with
  ten more crates. `openat2(RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS)` gives the rule in one call,
  where the kernel and its seccomp profile have it and nowhere on macOS, which would mean a
  second code path as its fallback. The walk has the same meaning with calls every Unix has,
  one code path, and no `unsafe`. The reference connectors need a Unix.
- **Only regular files are read, within a size.** A file is opened without blocking and its
  descriptor decides what it is: a link, a pipe, a device, a socket or a directory is refused as
  `not_a_regular_file`. Manifests (128 MiB), catalog versions (16 MiB), owner files (128 bytes)
  and keeper files (4 MiB) are measured before they are read and never written larger. A source
  file is at most `max_file_bytes` (16 GiB).
- **Names are checked before they are used.** A table name is an identifier of the destination:
  ASCII letters, digits and underscores, at most 128 bytes; any other name, from the host or
  from a manifest, is refused as `invalid_name` or `manifest_invalid`. A manifest lists files
  relative to its pipeline's directory, each under `staging`; a manifest whose version is not
  its file's, or that names anything else, is refused. Versions are counted with checks, and an
  open or a schema change that keeps losing to other sessions ends after 64 tries.
- **Arrow files are read by the wire's decoder.** The footer and every block it lists are
  checked against the file before anything is held: lengths are not negative, blocks lie
  between the head and the footer, each after the block listed before it, and each within the
  frame limit.
  The schema and each block are then decoded by `rdlt-wire`'s `Decoder` under its limits, with
  its flatbuffers depth, so a schema nested to the limit reads back. A file is written only with
  a schema those limits admit and in batches of about 8 MiB, each checked against the frame
  limit. Compressed files, dictionary deltas and files from before Arrow 0.15 are refused.
  The reference crate depends on `rdlt-wire` for the decoder, which `cargo xtask deps` allows.
- **JSON lines are read a bounded line at a time.** The source pushes lines as they are written
  (`max_line_bytes`, 32 MiB, per line; `batch_rows` records and 64 MiB per push), so the engine's
  shredder types them and no integer becomes a float; the source infers no schema. The
  destination reads its own lines back under the table's schema with the same line limit, and
  refuses to write a line beyond it.
- **A float JSON has no number for is named.** JSON lines hold `NaN`, `Infinity` and `-Infinity`
  as strings, as the engine's JSON lowering does (ADR 0011), and read them back as floats.
- **What the destination creates is its user's alone.** Directories are 0700 and files 0600.
  `_rdlt` must belong to the user the destination runs as and be writable by no other, or the
  destination refuses the root. Published files are private too: a reader runs as that user.
- **Locks are private, exclusive and waited for a bounded time.** A table's lock file is created
  exclusively, opened without following a link, and must be a regular file of the user's. The
  lock is tried until `lock_wait_ms` (30 seconds) passed, then the call fails as a transient
  error coded `lock_timeout`.
- **A release removes only what is still dropped.** Under the table's lock, a release reads the
  latest manifest again and removes the catalog only while that manifest lists the table as
  dropped: a session a newer one overtook removes nothing the newer one created.
- **Temporaries are exclusive, unpredictable and removed.** A temporary is created with
  `O_EXCL` under a name of 128 random bits and removed on every path that does not publish it;
  an open removes those older than an hour. The write probe is one. A keeper's temporaries carry
  its file's name, and opening the keeper removes them.
- **A commit removes what it supersedes.** Once its manifest is durable, a commit removes the
  files the manifest before it listed, or the commit read, that the latest manifest does not
  list, with the directories of their own that leaves empty. A commit that fails removes what it
  wrote unless its manifest exists. What a crash leaves is removed by the next open, as before.
- **Lists stay short.** A table's catalog keeps 8 versions behind the latest, as manifests do. A
  manifest keeps the 16 latest receipts of each of 16 loads, and refuses a commit older than
  those (`receipt_forgotten`) rather than publish it twice. An append table's commit merges the
  files at the end of its list while each holds at most twice the rows after it, up to 64 MiB a
  file, keeping row order: the list grows with the logarithm of the table's rows, plus one file
  per 64 MiB. Only files of one format merge, Arrow files of one schema without dictionaries. A
  merge that fails changes nothing.
- **The keeper is bounded.** A keeper holds 4096 positions and writes its file only when a
  position moves.

## Consequences

A name, a link or a manifest no longer leads the connectors out of their roots, and no file they
read costs more than its limit before it is refused. The connectors are confined, not
tamper-proof: whoever may write the private directory as the destination's user can still change
what the tables hold.

What remains by design:

- A merge table's commit holds the whole table in memory while it merges it, and rewrites it.
- A staged batch is its own file in its own directories until its commit merges or publishes it.
- The manifest lists one file per 64 MiB of an append table, and is rewritten at each commit.
- A reader that read an older manifest may find a file gone, as `file_missing`, and reads the
  latest manifest again.
- The default keeper of the log and change sources is shared by every source of a process that
  names none: certification reads a source's position through a second connection, which only
  a shared keeper serves.
- A keeper file is one process's: two processes naming one file overwrite each other.

This amends ADR 0006: manifest paths are relative to the pipeline's directory, the destination's
files are private, catalogs keep 9 versions, and JSON lines are no longer read with an inferred
schema.
