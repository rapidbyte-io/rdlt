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
  a tree unlinks a link and never enters it, and enters at most 32 directories deep. The
  configured root itself is opened as the operator wrote it, links in that path followed: the
  operator names the root, nobody else does.
- **A root is its user's alone to write, and so is every directory beneath it.** The source's
  root, the destination's root and its `_rdlt`, the directory of a keeper file, and every
  directory entered beneath them must belong to the user the process runs as and be writable by
  neither its group nor others, whatever the sticky bit: mode 0755 passes, 0775 and 1777 do
  not. The open directory is asked, not its name, and a directory that fails is a
  configuration error coded `not_private` naming the directory, its owner and its mode. A
  directory on another file system than the directory it was reached from is refused too, as a
  mount point. Whoever cannot write those directories cannot create, replace or rename an entry
  in them, which is what makes a name beneath them mean one thing between two calls.
- **Files are not asked whose they are.** A regular file beneath a private directory is read
  whoever owns it and however many names it has. A hard link is a second name for a file its
  owner let be linked; only the user of the directory could have put the name there. Access
  control lists are not read: an operator who grants another user write access through one has
  shared the directory, and the connectors do not see it.
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
  file is at most `max_file_bytes` (16 GiB), counted while it is read: a file that grows past the
  limit under its reader fails the read and is never cut short. A cursor that stands beyond the
  end of its file fails as `cursor_beyond_file`: the file is not the one the cursor was taken
  from. Two entries of a source root that name one stream are refused as `duplicate_stream`.
- **Names are checked before they are used.** A table name is an identifier of the destination:
  lower-case ASCII letters, digits and underscores, at most 128 bytes, so no two names are one
  entry on a file system that folds case; any other name, from the host or
  from a manifest, is refused as `invalid_name` or `manifest_invalid`. A manifest lists files
  relative to its pipeline's directory, each under `staging`; a manifest whose version is not
  its file's, or that names anything else, is refused. A manifest, a catalog version or an owner
  file that does not read as one is a data error coded `manifest_invalid` or `catalog_invalid`.
  Versions are counted with checks, and an
  open or a schema change that keeps losing to other sessions ends after 64 tries.
- **Arrow files are read by the wire's decoder.** The footer and every block it lists are
  checked against the file before anything is held: lengths are not negative, blocks lie
  between the end of the schema message and the footer, each after the block listed before it,
  no block of either list sharing a byte with another, and each within the frame limit. The
  first message starts within the 64 bytes writers align to, behind zeros only.
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
  Published files are private too: a reader runs as that user.
- **Locks are private, exclusive and waited for a bounded time.** A table's lock file is created
  exclusively, opened without following a link, and must be a regular file of the user's. The
  lock is tried until `lock_wait_ms` (30 seconds) passed, then the call fails as a transient
  error coded `lock_timeout`. The lock is polled and keeps no queue: a waiter may be overtaken,
  and waits at most that long.
- **A release removes only what is still dropped.** Under the table's lock, a release reads the
  latest manifest again and removes the catalog only while that manifest lists the table as
  dropped and lists no files or schema for it: a session a newer one overtook removes nothing
  the newer one created, and a table dropped and created again keeps the catalog it was created
  with.
- **Temporaries are exclusive, unpredictable and removed.** A temporary is created with
  `O_EXCL` under a name of 128 random bits and removed on every path that does not publish it;
  an open removes those older than an hour, and removes files only, never a directory. The write
  probe is one. A keeper's temporaries carry its file's name, and opening the keeper removes
  them.
- **A commit's files are named for that commit alone.** Every file a commit writes, merging a
  table's rows, its tombstones or the files at the end of a list, carries the commit's load,
  its sequence and 128 random bits in its name. A staged file is named by its session's epoch,
  its load, its segment and a number the session counts. Every file is created exclusively. A
  commit that is retried writes new files, so cleaning up after an attempt never removes, by
  name, a file another attempt published.
- **A commit removes what it supersedes.** Once its manifest is durable, a commit removes the
  files the manifest before it listed, or the commit read, that the latest manifest does not
  list, with the directories of their own that leaves empty. A commit that fails removes what it
  wrote that the latest manifest does not list. What a crash leaves is removed by the next open,
  as before. A reader that finds a listed file gone reads the latest manifest again, a bounded
  number of times, and fails with `file_missing` only when the manifest it read is still the
  latest. A discard of what older sessions staged makes the latest manifest's name durable
  before it removes anything: a manifest a power loss could take back decides nothing.
- **Every durable step is ordered.** A file is synced before it is linked or renamed into
  place, and its directory after; a commit answered from its receipt syncs its manifests'
  directory again before it answers, since the commit it repeats may have failed between the
  link and the sync, and removes nothing where that sync fails. Tests record each step once it
  has taken place and assert the order, and die at each step in turn of every kind of commit,
  of an open, of a schema change and of a keeper's write. What they cannot see is whether a
  sync that was called reached the disk: a call recorded and not made, or made on the wrong
  descriptor, passes every test, and so does a file system that does not keep its promise. A
  directory whose creation could not be made durable is taken as durable when the call is made
  again: after a sync has failed, nothing a caller does makes what came before it certain.
- **Nothing is written that its reader refuses.** An Arrow file is written as batches of about
  8 MiB and of at most the rows a reader takes in one, each checked against every limit the
  reader holds a batch to: its frame, the values of each dictionary, which a reader takes as a
  batch of its own, and nested values that take no bytes. A batch no reader accepts, one row
  larger than a frame among them, fails the write and leaves no file. JSON lines hold a
  dictionary's rows as the values they stand for, null where the key is null or the value it
  stands for is.
- **Lists stay short.** A table's catalog keeps 8 versions behind the latest, as manifests do. A
  manifest keeps every receipt of each of its 16 latest loads: a commit repeated however far
  back in its load is answered with its receipt, and how many receipts a load may hold is the
  engine's to bound, not the destination's. An append table's commit merges the
  files it adds with each other, whatever rows each holds, and a file listed before them with
  those after it while it holds at most twice their rows, never reading more than 64 MiB into
  one file and keeping row order. Every two files next to each other in a list then either at
  least halve in rows or were together more than 64 MiB when they were listed. So a table
  whose files never reach that size lists at most as many files as the logarithm of its rows,
  and one more; a larger table lists at most that many for each 32 MiB it holds. A row is
  written again only into a file half as large again as the file it was in, so merges write a
  table over at most twice and about 1.7 times the base-two logarithm of its rows, not once a
  commit. Tests hold
  both bounds for steady sources of several shapes and for random ones. The bound is on what a
  merge reads: only files of one format and one schema merge, a change of schema starting a
  run of its own, and dictionary columns merge as their values, since each file carries
  dictionaries of its own, so a file merged from dictionaries of long values is larger than
  what it was read from. A dictionary inside a map, a union, a run-end column or a list view
  is not written as its values, and files that hold one merge with none. A merge that fails
  leaves the list as it was and removes what it wrote; the commit goes on, and the next
  commit tries again, at the cost of opening and reading those files once more. Each commit
  opens the files it may merge to compare their schemas, and a commit answered from its
  receipt walks its session's staged tree: both are bounded by the list.
- **The keeper is bounded, private and durable before it moves.** A keeper holds 4096
  positions. An acknowledgement that moves a position writes the whole file through a temporary,
  synced and renamed, and the keeper stands at the position only once that write is durable: a
  write that fails leaves it where it stood. An acknowledgement at or behind the position
  writes nothing. The file's directory must exist and be private as a root is, and the file a
  regular file of the user's that no other may write, named by text. Keepers of one process
  are told apart by the directory itself and the file's name, so two spellings of one path are
  one keeper. A keeper holds an exclusive lock on a file beside its own for as long as it
  lives, so a second process naming the file is refused and never writes its positions back.
- **A root stays where it was opened.** The destination holds its root from its first open, and
  each call asks whether the root's path still leads to that directory: a root moved aside,
  removed or replaced fails as a configuration error coded `root_replaced` and takes no write
  where nobody looks for it.

## Consequences

A name, a link or a manifest no longer leads the connectors out of their roots, and no file they
read costs more than its limit before it is refused. The connectors are confined, not
tamper-proof: whoever may write the private directory as the destination's user can still change
what the tables hold.

What remains by design:

- A merge table's commit holds the whole table in memory while it merges it, and rewrites it.
- A staged batch is its own file in its own directories until its commit merges or publishes it.
- The manifest lists one file per 64 MiB of an append table, and is rewritten at each commit.
- A reader holding one of the 8 older manifests finds the manifest but may find its files gone:
  older manifests are kept for their receipts and their lists, not their data.
- The mount-point rule compares file systems, so a destination root may not hold a mount or a
  subvolume, which has a device number of its own, and a bind mount of the same file system is
  not seen.
- The private rule holds for a source root too: a directory another user owns is refused even
  where this user may only read it, so a source reads what its own user keeps.
- An Arrow batch is cut by its rows' average size, so rows of very uneven size can make a batch
  larger than a frame, which fails the write.
- The default keeper of the log and change sources is shared by every source of a process that
  names none: certification reads a source's position through a second connection, which only
  a shared keeper serves.
- A keeper file is one process's, for as long as the process runs: a second process naming it
  fails to connect until the first is gone.

This amends ADR 0006: manifest paths are relative to the pipeline's directory, the destination's
files are private, catalogs keep 9 versions, and JSON lines are no longer read with an inferred
schema.
