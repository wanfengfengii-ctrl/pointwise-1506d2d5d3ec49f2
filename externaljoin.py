"""Bounded-memory external sort/merge join for ``(int key, str payload)`` streams.

The public entry point is :func:`join`.  It consumes each input exactly once,
writes only sorted intermediate records to the caller supplied ``workdir`` and
yields the join result lazily.

Key facts
---------
* Keys are encoded into self-delimiting, lexicographically sortable byte
  strings.  Concatenating the sortable key with ``json.dumps``-safe UTF-8
  payload bytes gives records whose byte order is the required integer key
  order.
* Input is turned into sorted runs whose raw record bytes stay within
  ``buffer_bytes``.  Runs are merged with a bounded fan-in (never opening
  more than a small, budget-derived number of files at once).
* Groups are never materialised: a matching group is emitted as a nested
  stream.  The right side of a group is re-read from its sorted file for each
  left record.  Thus a hot group of any size and a Cartesian product of any
  size can be streamed with memory that depends on one record, not on the
  group or product size.
* Only sorted inputs are written to disk.  Pair results are not written to
  disk.
"""

from __future__ import annotations

import contextlib
import heapq
import os
import struct
import tempfile

__all__ = ["join"]

_MASK64 = (1 << 64) - 1
_LEN = struct.Struct(">I")

# Reader/writer buffer sizes.  They are deliberately small and constant; the
# number of simultaneously open merge readers is derived from buffer_bytes.
_IO_BUFFER = 16 * 1024
_WRITE_BUFFER = 64 * 1024


# ---------------------------------------------------------------------------
# Sortable integer encoding
# ---------------------------------------------------------------------------
#
# Layout:  sign(1 byte) | n(8 bytes) | n magnitude bytes
#
# Positive: sign=1, n = byte length of the magnitude, magnitude big-endian.
# Negative: sign=0, field = MASK64 - n, magnitude byte-complemented.
#
# The 9-byte header makes the encoding prefix free (a shorter magnitude is
# never a prefix of a longer magnitude with the same sign/length field), and
# unsigned byte comparison of the complete encoding agrees with integer order.

def _encode_key(key):
    if key < 0:
        mag = -key
        n = (mag.bit_length() + 7) // 8
        body = bytes(255 - byte for byte in mag.to_bytes(n, "big"))
        return b"\x00" + (_MASK64 - n).to_bytes(8, "big") + body
    n = (key.bit_length() + 7) // 8
    return b"\x01" + n.to_bytes(8, "big") + key.to_bytes(n, "big")


def _decode_key(encoded):
    sign = encoded[0]
    field = int.from_bytes(encoded[1:9], "big")
    if sign:
        n = field
        return int.from_bytes(encoded[9:9 + n], "big") if n else 0
    n = _MASK64 - field
    body = encoded[9:9 + n]
    return -(((1 << (8 * n)) - 1) - int.from_bytes(body, "big"))


def _payload_bytes(payload):
    # surrogatepass keeps an exact round trip even for strings containing lone
    # surrogates, which json.dumps (default ensure_ascii=True) can serialise.
    return payload.encode("utf-8", "surrogatepass")


def _payload_text(payload):
    return payload.decode("utf-8", "surrogatepass")


# ---------------------------------------------------------------------------
# Record readers and temporary-file housekeeping
# ---------------------------------------------------------------------------

def _read_exact(stream, size):
    """Read exactly *size* bytes; return None only for an immediate EOF."""
    if size == 0:
        return b""
    first = stream.read(size)
    if not first:
        return None
    if len(first) == size:
        return first
    parts = [first]
    remaining = size - len(first)
    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            raise EOFError("truncated external-join temporary file")
        parts.append(chunk)
        remaining -= len(chunk)
    return b"".join(parts)


class _RecordReader:
    """Sequential reader for ``key_enc | payload_len(4) | payload`` records."""

    __slots__ = ("stream", "offset")

    def __init__(self, stream, offset=0):
        self.stream = stream
        self.offset = offset  # logical byte offset of the next record

    def read(self):
        header = _read_exact(self.stream, 9)
        if header is None:
            return None
        sign = header[0]
        field = int.from_bytes(header[1:9], "big")
        n = field if sign else _MASK64 - field
        body = _read_exact(self.stream, n)
        if body is None:
            raise EOFError("truncated external-join key")
        key_enc = header + body
        length_bytes = _read_exact(self.stream, 4)
        if length_bytes is None:
            raise EOFError("truncated external-join record header")
        payload_len = _LEN.unpack(length_bytes)[0]
        payload = _read_exact(self.stream, payload_len)
        if payload is None:
            raise EOFError("truncated external-join payload")
        self.offset += 9 + n + 4 + payload_len
        return key_enc, payload, 9 + n + 4 + payload_len


class _TempStore:
    """Owns every file created by one join; cleans up without touching others."""

    def __init__(self, workdir):
        self.workdir = workdir
        self.paths = []
        self.handles = set()

    def new_path(self, tag):
        fd, path = tempfile.mkstemp(
            prefix=".externaljoin-", suffix="-" + tag + ".tmp", dir=self.workdir
        )
        os.close(fd)
        self.paths.append(path)
        return path

    def open(self, path, mode, buffering=-1):
        handle = open(path, mode, buffering=buffering)
        self.handles.add(handle)
        return handle

    @contextlib.contextmanager
    def writer(self, path):
        handle = self.open(path, "wb", buffering=_WRITE_BUFFER)
        try:
            yield handle
        finally:
            self.close_handle(handle)

    def close_handle(self, handle):
        try:
            handle.close()
        finally:
            self.handles.discard(handle)

    def discard(self, path):
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        try:
            self.paths.remove(path)
        except ValueError:
            pass

    def cleanup(self):
        for handle in list(self.handles):
            try:
                handle.close()
            except Exception:
                pass
        self.handles.clear()
        for path in self.paths:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
            except OSError:
                pass
        self.paths = []


# ---------------------------------------------------------------------------
# Sorted run creation and bounded-fan-in merge
# ---------------------------------------------------------------------------

def _spill_sorted_runs(items, store, runs, budget):
    """Consume *items* once, feeding byte-sorted runs into *runs* (a _RunSet).

    An in-memory run is spilled as soon as adding the next record would exceed
    *budget* raw bytes, so the live buffer stays bounded even for adversarial
    key distributions.
    """
    records = []
    size = 0

    def spill():
        records.sort()
        path = store.new_path("run")
        with store.writer(path) as out:
            out.writelines(records)
        runs.add(path)

    for key, payload in items:
        payload_bytes = _payload_bytes(payload)
        frame = _encode_key(key) + _LEN.pack(len(payload_bytes)) + payload_bytes
        # A single record is at most ~8 KiB and budget >= 1 MiB, so a record
        # always fits; the check also keeps the invariant for defensive use.
        if records and size + len(frame) > budget:
            spill()
            records = []
            size = 0
        records.append(frame)
        size += len(frame)

    if records:
        spill()


def _merge_run_group(paths, out_path, store):
    """Merge *paths* (all sorted) into *out_path* using a heap of readers."""
    readers = []
    heap = []
    try:
        for index, path in enumerate(paths):
            reader = _RecordReader(store.open(path, "rb", buffering=_IO_BUFFER))
            readers.append(reader)
            record = reader.read()
            if record is not None:
                key_enc, payload, _ = record
                heapq.heappush(heap, (key_enc, index, payload, reader))

        with store.writer(out_path) as out:
            while heap:
                key_enc, _index, payload, reader = heapq.heappop(heap)
                out.write(key_enc)
                out.write(_LEN.pack(len(payload)))
                out.write(payload)
                record = reader.read()
                if record is not None:
                    next_key, next_payload, _ = record
                    heapq.heappush(heap, (next_key, _index, next_payload, reader))
    finally:
        for reader in readers:
            store.close_handle(reader.stream)


def _fan_in(budget):
    """Fan-in whose readers plus heap fit comfortably inside *budget*.

    Each reader carries a 16 KiB I/O buffer and the heap holds at most one
    key plus one payload (<= ~8 KiB) per reader.  64 KiB of budget per input
    is therefore a safe, conservative exchange rate.
    """
    return max(2, min(32, int(budget) // (64 * 1024)))


class _RunSet:
    """Online bounded-fan-in run compaction.

    Runs are folded level by level as soon as ``fan_in`` of them accumulate.
    Memory for bookkeeping is O(fan_in * log_fan_in(runs)) paths and live
    temporary data stays within a constant factor of the encoded input.
    """

    def __init__(self, store, budget, tag):
        self.store = store
        self.fan_in = _fan_in(budget)
        self.tag = tag
        self.levels = []

    def add(self, path):
        level = 0
        while True:
            while len(self.levels) <= level:
                self.levels.append([])
            bucket = self.levels[level]
            bucket.append(path)
            if len(bucket) < self.fan_in:
                return
            self.levels[level] = []
            out_path = self.store.new_path(
                "%s-L%d-%d" % (self.tag, level, len(bucket))
            )
            _merge_run_group(bucket, out_path, self.store)
            for old in bucket:
                self.store.discard(old)
            path = out_path
            level += 1

    def finish(self):
        pending = []
        for bucket in self.levels:
            pending.extend(bucket)
        self.levels = []
        if not pending:
            return None
        if len(pending) == 1:
            return pending[0]

        fan_in = self.fan_in
        current = pending
        round_index = 0
        while len(current) > fan_in:
            merged = []
            for start in range(0, len(current), fan_in):
                chunk = current[start:start + fan_in]
                if len(chunk) == 1:
                    merged.append(chunk[0])
                    continue
                out_path = self.store.new_path(
                    "%s-r%d" % (self.tag, round_index)
                )
                _merge_run_group(chunk, out_path, self.store)
                for old in chunk:
                    self.store.discard(old)
                merged.append(out_path)
            current = merged
            round_index += 1
        if len(current) == 1:
            return current[0]
        out_path = self.store.new_path(self.tag + "-final")
        _merge_run_group(current, out_path, self.store)
        for old in current:
            self.store.discard(old)
        return out_path


def _run_budget(buffer_bytes):
    # buffer_bytes is the raw-byte allowance for an in-memory sorted run.
    # Python object overhead is a small constant multiple of the raw size
    # (frames are bytes objects in one list), keeping the peak below the
    # documented 8x + 2 MiB envelope.
    return int(buffer_bytes)


# ---------------------------------------------------------------------------
# Streaming group traversal
# ---------------------------------------------------------------------------

class _Cursor:
    __slots__ = ("reader", "_loaded", "_record", "record_start")

    def __init__(self, reader):
        self.reader = reader
        self._loaded = False
        self._record = None
        self.record_start = None

    def peek(self):
        if not self._loaded:
            self.record_start = self.reader.offset
            self._record = self.reader.read()
            self._loaded = True
        return self._record

    def pop(self):
        record = self.peek()
        self._loaded = False
        self._record = None
        return record


def _iter_join(left_runs, right_runs, store):
    left_path = left_runs.finish()
    right_path = right_runs.finish()

    left_cursor = None
    right_cursor = None
    if left_path is not None:
        left_cursor = _Cursor(
            _RecordReader(store.open(left_path, "rb", buffering=_IO_BUFFER))
        )
    if right_path is not None:
        right_cursor = _Cursor(
            _RecordReader(store.open(right_path, "rb", buffering=_IO_BUFFER))
        )

    # One reusable reader over the right sorted file, seeked to the beginning
    # of the current group for each left record.
    right_group_reader = None
    if right_path is not None:
        right_group_reader = _RecordReader(
            store.open(right_path, "rb", buffering=_IO_BUFFER)
        )

    while True:
        left = left_cursor.peek() if left_cursor is not None else None
        right = right_cursor.peek() if right_cursor is not None else None

        if left is None and right is None:
            return

        if left is None:
            # Right-only tail group.
            group_key = right[0]
            key = _decode_key(group_key)
            while True:
                record = right_cursor.peek()
                if record is None or record[0] != group_key:
                    break
                right_cursor.pop()
                yield (key, None, _payload_text(record[1]))
            continue

        if right is None:
            # Left-only tail group.
            group_key = left[0]
            key = _decode_key(group_key)
            while True:
                record = left_cursor.peek()
                if record is None or record[0] != group_key:
                    break
                left_cursor.pop()
                yield (key, _payload_text(record[1]), None)
            continue

        if left[0] < right[0]:
            group_key = left[0]
            key = _decode_key(group_key)
            while True:
                record = left_cursor.peek()
                if record is None or record[0] != group_key:
                    break
                left_cursor.pop()
                yield (key, _payload_text(record[1]), None)
            continue

        if right[0] < left[0]:
            group_key = right[0]
            key = _decode_key(group_key)
            while True:
                record = right_cursor.peek()
                if record is None or record[0] != group_key:
                    break
                right_cursor.pop()
                yield (key, None, _payload_text(record[1]))
            continue

        # Matching group.  Emit a left-major Cartesian product while holding
        # at most one left record and one right record in memory.
        group_key = left[0]
        key = _decode_key(group_key)
        group_start = right_cursor.record_start

        while True:
            left_record = left_cursor.peek()
            if left_record is None or left_record[0] != group_key:
                break
            left_cursor.pop()
            left_text = _payload_text(left_record[1])

            right_group_reader.stream.seek(group_start)
            right_group_reader.offset = group_start
            while True:
                right_record = right_group_reader.read()
                if right_record is None or right_record[0] != group_key:
                    break
                yield (key, left_text, _payload_text(right_record[1]))

        # Advance the primary right cursor past the group it supplied.
        while True:
            record = right_cursor.peek()
            if record is None or record[0] != group_key:
                break
            right_cursor.pop()

    # `while True` only exits through `return`; the trailing cleanup happens
    # in the generator's finally block in join().


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def join(left, right, workdir, buffer_bytes):
    """Join two single-pass ``(int, str)`` streams from *workdir*.

    Parameters
    ----------
    left, right:
        Iterables of ``(int key, str payload)``.  Each is consumed exactly
        once.  Exceptions raised by the source propagate unchanged.
    workdir:
        Existing directory used for private temporary files.  Pre-existing
        entries are never touched or removed.
    buffer_bytes:
        Raw-byte budget (1 .. 16 MiB) for an in-memory sorted run.

    Returns
    -------
    A closable iterator yielding ``(key, left_payload_or_None,
    right_payload_or_None)`` in ascending key order.  For a key occurring
    ``m`` times on the left and ``n`` times on the right there are ``m*n``
    Cartesian pairs; unmatched occurrences are emitted with ``None`` on the
    other side.  Group-internal order is unspecified.

    The iterator owns its temporary files.  They are removed on exhaustion,
    on an exception from either source, and on an early ``close()``.
    """
    return _join_generator(left, right, os.fspath(workdir), int(buffer_bytes))


def _join_generator(left, right, workdir, buffer_bytes):
    store = _TempStore(workdir)
    traversal = None
    try:
        budget = _run_budget(buffer_bytes)
        left_runs = _RunSet(store, budget, "left")
        right_runs = _RunSet(store, budget, "right")
        _spill_sorted_runs(left, store, left_runs, budget)
        _spill_sorted_runs(right, store, right_runs, budget)
        traversal = _iter_join(left_runs, right_runs, store)
        for item in traversal:
            yield item
    finally:
        # Robust on exhaustion, source exceptions and GeneratorExit (early
        # close).  Close the traversal generator deterministically before the
        # store so readers and files are released in a known order; cleanup
        # errors must not mask the original outcome.
        if traversal is not None:
            try:
                traversal.close()
            except Exception:
                pass
        store.cleanup()
