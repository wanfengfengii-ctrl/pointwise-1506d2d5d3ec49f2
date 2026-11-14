# External join — resource notes

`externaljoin.join(left, right, workdir, buffer_bytes)` implements a
single-pass, bounded-memory sort–merge join for two streams of
`(int key, str payload)` records. Only the Python standard library is used.

## Files

| File | Purpose |
| --- | --- |
| `externaljoin.py` | The `join` implementation. |
| `verify_join.py` | Independent verifier; derives every expectation from the inputs. |
| `demo_reconcile.py` | Realistic ledger × processor reconciliation demo. |
| `.pointwise-runtime.json` | Runtime manifest: `version=1`, `kind=cli`, `setup`/`verify`/`demo` argv arrays. |

Run:

```
python3 -I verify_join.py
python3 -I demo_reconcile.py
```

The verifier and demo are independent in the sense that the verifier never
imports the demo and recomputes expected pairs itself; the demo imports only
the public join.

## Algorithm

1. **Key encoding.** Each integer key becomes a self-delimiting, prefix-free
   byte string: `sign (1) | length field (8) | magnitude bytes`. Positive
   magnitudes are big-endian; negative magnitudes are byte-complemented with
   inverted length. Unsigned byte order on the encoding equals integer order,
   so a record is simply `encoded_key | payload_len (4) | utf-8 payload` and
   plain byte comparison sorts records by key.
2. **Run generation (single pass).** Records accumulate in a list until the
   next record would push raw record bytes past `buffer_bytes`; the list is
   then sorted by byte order and written as one run. Both inputs are read
   exactly once.
3. **Online compaction.** Runs are folded level by level as soon as `fan_in`
   accumulate (`fan_in = clamp(buffer_bytes // 64 KiB, 2, 32)`); leftover runs
   are merged in tail rounds. Every merge is a heap over one reader per run,
   so at most `fan_in` files are open at once.
4. **Streaming group traversal.** The two final sorted files are traversed
   with one-record lookahead. Unmatched occurrences are emitted directly. For
   a matching key the right sorted file is seeked back to the group start for
   each left occurrence, producing `m × n` pairs without materialising the
   group or the product. A single right reader is reused, so a hot group of
   any size needs O(1) memory.

## Memory

Let *live output* mean the pair currently being yielded. Peak additional
Python allocation is measured by the verifier's `tracemalloc` check and by
standalone stress tests.

* Buffered records: raw bytes ≤ `buffer_bytes`; per-record overhead (bytes
  object header, list slot) is a small constant multiple.
* Merge fan-in: each reader carries a 16 KiB I/O buffer plus ≤ one key and one
  payload (≤ 8 KiB) in the heap; the chosen fan-in keeps this within the
  `buffer_bytes` budget.
* Group traversal: two one-record lookaheads plus one reusable reader, i.e.
  a few records plus fixed I/O buffers.
* Pair results: generated and yielded, never buffered. Group size, product
  size and total input size never enter the live set.

Measured peak additional allocation (including the source generators and
all bookkeeping, with tiny payloads — the worst per-record ratio):

| buffer_bytes | pairs | peak additional | ratio | envelope (8×buffer+2 MiB) |
| --- | --- | --- | --- | --- |
| 1 MiB | 4.5 M | ~4.0 MB | 3.85× | 10.0 MB |
| 4 MiB | 4.5 M | ~9.2 MB | 2.20× | 34.0 MB |

The hot-group test in the verifier uses the contractual minimum 1 MiB budget
with ~8 KiB payloads (the contractual maximum per item), so both sides spill
to multiple runs; the 150 × 200 Cartesian product (30 002 pairs including two
unmatched neighbours) streams with the same bounded live set. Disk sampling
confirms intermediate data stays at ~2.3 MB for ~9 MB of encoded input and
does not change while pairs are emitted.

## Temporary disk

* Written: sorted encoded input runs and intermediate merge outputs only.
* `O(1)` × encoded input: online compaction keeps at most one partial run
  per level (≤ `fan_in` runs each); tail merging duplicates one generation.
  Measured worst case is a small constant multiple of input bytes, verified
  as `peak_bytes ≤ 6 × encoded_input + 1 MiB` on the hot-group case.
* Pair results are never written. The verifier samples the work directory
  during output and asserts its size and file count do not change.
* Files are created with `tempfile.mkstemp(dir=workdir)` under a
  `.externaljoin-*` prefix. Only paths created by this join instance are
  unlinked; pre-existing entries are never read, modified or removed.

## Cleanup and errors

The public function returns a generator that owns its temporary store:

* **Exhaustion** — after the final pair, `finally` closes all readers and
  unlinks only its own files.
* **Source exception** — exceptions from `left` or `right` propagate
  unchanged (same object); the `finally` cleanup still runs.
* **Early `close()`** — `GeneratorExit` enters the same `finally`; files and
  file descriptors are released. Cleanup failures are swallowed so they cannot
  mask the original outcome.
* Pre-existing directory contents are preserved in all three cases; the
  verifier checks sentinel files and directories survive.

## Complexity

Let `N` be total input records and `R` the number of runs
(≤ `N · record_size / buffer_bytes`).

* Time: input pass O(N log(buffered)), merge O(N log R), output proportional
  to pairs plus, for matching groups, one re-seek of the right group per left
  occurrence.
* Space: memory as above; disk O(input bytes).
