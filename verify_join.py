#!/usr/bin/env python3
"""Independent verification for :func:`externaljoin.join`.

The verifier only uses the public ``join`` API.  It derives every expected
result from the generated inputs, and observes the work directory and the
process allocator to confirm the documented resource behaviour.

Exit status is 0 only when every check passes.
"""

import gc
import json
import os
import random
import sys
import tempfile
import tracemalloc
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from externaljoin import join  # noqa: E402

SENTINEL_FILE = "preexisting-note.txt"
SENTINEL_DIR = "preexisting-dir"

_failures = []


def check(condition, message):
    if condition:
        print("PASS  " + message)
    else:
        print("FAIL  " + message)
        _failures.append(message)


def leftovers(workdir):
    """Temporary files owned by the joiner (pre-existing entries are ignored)."""
    return sorted(
        name for name in os.listdir(workdir) if name.startswith(".externaljoin-")
    )


def expected_pairs(left_items, right_items):
    """Full Cartesian-product expectation, including unmatched records."""
    by_key_left = defaultdict(Counter)
    by_key_right = defaultdict(Counter)
    for key, payload in left_items:
        by_key_left[key][payload] += 1
    for key, payload in right_items:
        by_key_right[key][payload] += 1

    expected = Counter()
    for key in sorted(set(by_key_left) | set(by_key_right)):
        left_payloads = by_key_left.get(key, Counter())
        right_payloads = by_key_right.get(key, Counter())
        if left_payloads and right_payloads:
            for left_payload, left_count in left_payloads.items():
                for right_payload, right_count in right_payloads.items():
                    expected[(key, left_payload, right_payload)] += (
                        left_count * right_count
                    )
        elif left_payloads:
            for left_payload, left_count in left_payloads.items():
                expected[(key, left_payload, None)] += left_count
        else:
            for right_payload, right_count in right_payloads.items():
                expected[(key, None, right_payload)] += right_count
    return expected


def assert_sorted_and_complete(records, message):
    keys = [key for key, _left, _right in records]
    check(keys == sorted(keys), message + " (ascending keys)")
    check(
        all(isinstance(key, int) for key in keys),
        message + " (keys are ints)",
    )
    check(
        all(
            (left is None or isinstance(left, str))
            and (right is None or isinstance(right, str))
            for _key, left, right in records
        ),
        message + " (payloads are str/None)",
    )
    check(
        all(left is not None or right is not None for _key, left, right in records),
        message + " (no empty pairs)",
    )


class CountingSource:
    """Single-pass enforcement: records iteration calls and consumed items."""

    def __init__(self, items):
        self._items = list(items)
        self.iter_calls = 0
        self.consumed = 0

    def __iter__(self):
        self.iter_calls += 1
        if self.iter_calls > 1:
            raise AssertionError("source iterated more than once")
        return self

    def __next__(self):
        if self.consumed >= len(self._items):
            raise StopIteration
        item = self._items[self.consumed]
        self.consumed += 1
        return item


class DirProbe:
    """Samples the size and file count of a work directory."""

    def __init__(self, workdir):
        self.workdir = workdir
        self.max_files = 0
        self.max_bytes = 0
        self.output_first_bytes = None
        self.output_first_files = None
        self.output_growth = False
        self._output_samples = 0

    def _measure(self):
        files = 0
        total = 0
        with os.scandir(self.workdir) as entries:
            for entry in entries:
                if not entry.is_file(follow_symlinks=False):
                    continue
                files += 1
                total += entry.stat(follow_symlinks=False).st_size
        return files, total

    def sample_source(self):
        files, total = self._measure()
        self.max_files = max(self.max_files, files)
        self.max_bytes = max(self.max_bytes, total)

    def sample_output(self):
        self._output_samples += 1
        if self._output_samples % 5000 != 1:
            return
        files, total = self._measure()
        if self.output_first_bytes is None:
            self.output_first_bytes = total
            self.output_first_files = files
        elif total != self.output_first_bytes or files != self.output_first_files:
            self.output_growth = True


class ProbingSource:
    def __init__(self, items, probe):
        self._iterator = iter(items)
        self._probe = probe

    def __iter__(self):
        return self

    def __next__(self):
        item = next(self._iterator)
        self._probe.sample_source()
        return item


class BoomError(Exception):
    pass


# ---------------------------------------------------------------------------
# A. Required example and identical/missing side behaviour
# ---------------------------------------------------------------------------

def verify_contract_example(workdir):
    with open(os.path.join(workdir, SENTINEL_FILE), "w", encoding="utf-8") as out:
        out.write("keep me\n")
    os.mkdir(os.path.join(workdir, SENTINEL_DIR))

    left = CountingSource([(1, "a"), (1, "b")])
    right = CountingSource([(1, "x"), (2, "y")])
    result = list(join(left, right, workdir, 1 << 20))

    expected = Counter(
        [
            (1, "a", "x"),
            (1, "b", "x"),
            (2, None, "y"),
        ]
    )
    check(Counter(result) == expected, "required example produces the 3 pairs")
    assert_sorted_and_complete(result, "required example")
    check(
        sorted(os.listdir(workdir)) == [SENTINEL_DIR, SENTINEL_FILE],
        "required example leaves only pre-existing workdir entries",
    )
    check(left.iter_calls == 1 and right.iter_calls == 1, "each source iterated once")
    check(
        left.consumed == 2 and right.consumed == 2,
        "each source consumed exactly once",
    )

    for name, left_items, right_items in (
        ("empty/empty", [], []),
        ("left-only", [(5, "sole-left")], []),
        ("right-only", [], [(5, "sole-right")]),
    ):
        result = list(join(iter(left_items), iter(right_items), workdir, 1 << 20))
        check(
            Counter(result) == expected_pairs(left_items, right_items),
            "edge case " + name,
        )
        check(
            sorted(os.listdir(workdir)) == [SENTINEL_DIR, SENTINEL_FILE],
            "edge case " + name + " cleans up",
        )


# ---------------------------------------------------------------------------
# B. Randomised full-multiset comparison (duplicates, signs, extremes)
# ---------------------------------------------------------------------------

def verify_randomised(workdir):
    rng = random.Random(20261114)
    payload_pool = [
        "",
        "plain",
        "line\nbreak",
        'quote"and\\slash',
        "汉字🎉",
        "\ud800lone-surrogate",
        "x" * 3000,
    ]

    for trial in range(4):
        keys = [-10**40, -(1 << 200), -2, -1, 0, 1, 2, 1 << 200, 10**40]
        left_items = []
        right_items = []
        for index in range(rng.randint(0, 220)):
            key = rng.choice(keys) if index % 7 == 0 else rng.randint(-12, 12)
            if index % 3 == 0:
                payload = rng.choice(payload_pool)
            else:
                payload = "L%03d-%d" % (index, trial)
            left_items.append((key, payload))
        for index in range(rng.randint(0, 220)):
            key = rng.choice(keys) if index % 7 == 0 else rng.randint(-12, 12)
            if index % 4 == 0:
                payload = rng.choice(payload_pool)
            else:
                payload = "R%03d-%d" % (index, trial)
            right_items.append((key, payload))

        for buffer_bytes in (1024, 4096, 1 << 20):
            result = list(join(iter(left_items), iter(right_items), workdir, buffer_bytes))
            check(
                Counter(result) == expected_pairs(left_items, right_items),
                "random trial %d buffer %d matches reference"
                % (trial, buffer_bytes),
            )
            assert_sorted_and_complete(
                result, "random trial %d buffer %d" % (trial, buffer_bytes)
            )
            check(
                leftovers(workdir) == [],
                "random trial %d buffer %d cleans up"
                % (trial, buffer_bytes),
            )


# ---------------------------------------------------------------------------
# C. Hot group: both sides spill; complete Cartesian product, streamed
# ---------------------------------------------------------------------------

def verify_hot_group(workdir):
    hot_key = 7001
    left_count = 150
    right_count = 200
    # ~8 KiB payloads are the contractual maximum per item, so a 1 MiB
    # buffer (the contractual minimum) forces spills well below the group
    # size while still exercising only documented buffer values.
    left_items = [
        (hot_key, "L%04d-" % i + "l" * 8170) for i in range(left_count)
    ]
    right_items = [
        (hot_key, "R%04d-" % i + "r" * 8170) for i in range(right_count)
    ]
    left_items.append((hot_key - 1, "left-only-neighbour"))
    right_items.append((hot_key + 1, "right-only-neighbour"))

    buffer_bytes = 1 << 20
    probe = DirProbe(workdir)
    produced = 0
    left_seen = Counter()
    right_seen = Counter()
    previous_key = None
    order_ok = True
    group_pairs = 0

    iterator = join(
        ProbingSource(left_items, probe),
        ProbingSource(right_items, probe),
        workdir,
        buffer_bytes,
    )
    try:
        for key, left_payload, right_payload in iterator:
            probe.sample_output()
            if previous_key is not None and key < previous_key:
                order_ok = False
            previous_key = key
            produced += 1
            if key == hot_key:
                group_pairs += 1
                left_seen[left_payload] += 1
                right_seen[right_payload] += 1
    finally:
        iterator.close()

    input_bytes = sum(
        len(payload.encode("utf-8", "surrogatepass")) + 24
        for _key, payload in left_items + right_items
    )
    expected_pairs_count = left_count * right_count + 2  # two unmatched neighbours

    check(order_ok, "hot group output key order is ascending")
    check(
        produced == expected_pairs_count,
        "hot group emits %d pairs (got %d)" % (expected_pairs_count, produced),
    )
    check(
        group_pairs == left_count * right_count,
        "hot group Cartesian product is complete",
    )
    check(
        all(left_seen["L%04d-" % i + "l" * 8170] == right_count for i in range(left_count)),
        "each left occurrence is paired with every right occurrence",
    )
    check(
        all(right_seen["R%04d-" % i + "r" * 8170] == left_count for i in range(right_count)),
        "each right occurrence is paired with every left occurrence",
    )
    check(
        probe.max_files >= 2,
        "hot group spilled to multiple temporary files (both sides exceed the 1 MiB run budget)",
    )
    check(
        probe.max_bytes <= 6 * input_bytes + (1 << 20),
        "temporary disk stays O(1) x encoded input (%d <= %d)"
        % (probe.max_bytes, 6 * input_bytes + (1 << 20)),
    )
    check(
        probe.output_first_bytes is not None and not probe.output_growth,
        "temporary disk does not grow while pairs are emitted (no pair output on disk)",
    )
    check(leftovers(workdir) == [], "hot group cleans up temporary files")


# ---------------------------------------------------------------------------
# D. Cleanup under early close and source exceptions
# ---------------------------------------------------------------------------

def verify_cleanup(workdir):
    with open(os.path.join(workdir, SENTINEL_FILE), "w", encoding="utf-8") as out:
        out.write("survive\n")

    left_items = [(9, "L%04d" % i + "z" * 300) for i in range(200)]
    right_items = [(9, "R%04d" % i + "z" * 300) for i in range(200)]

    iterator = join(iter(left_items), iter(right_items), workdir, 1024)
    pulled = [next(iterator) for _ in range(25)]
    check(len(pulled) == 25, "early-close run produced initial pairs")
    check(
        any(name.startswith(".externaljoin-") for name in os.listdir(workdir)),
        "early-close run created temporary files before close",
    )
    iterator.close()
    check(
        leftovers(workdir) == [],
        "early close removes only its own temporary files",
    )

    for side in ("left", "right"):
        boom = BoomError("boom from " + side)

        def exploding(boom=boom, side=side):
            yield (1, "first")
            yield (1, "second")
            raise boom

        good = iter([(1, "ok")])
        try:
            list(join(exploding(), good, workdir, 1024) if side == "left"
                 else join(good, exploding(), workdir, 1024))
        except BoomError as caught:
            check(caught is boom, "source exception propagates unchanged from " + side)
        else:
            check(False, "source exception propagates unchanged from " + side)
        check(
            leftovers(workdir) == [] and SENTINEL_FILE in os.listdir(workdir),
            "source exception from " + side + " cleans up temporary files",
        )


# ---------------------------------------------------------------------------
# E. Peak allocation envelope (8 x buffer_bytes + 2 MiB)
# ---------------------------------------------------------------------------

def verify_memory_envelope():
    buffer_bytes = 1 << 20
    item_count = 50000

    def stream():
        for index in range(item_count):
            yield (index, "x")

    workdir = tempfile.mkdtemp(prefix="externaljoin-verify-mem-")
    try:
        gc.collect()
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]
        tracemalloc.reset_peak()
        count = 0
        previous = None
        for key, _left, _right in join(stream(), stream(), workdir, buffer_bytes):
            if previous is not None and key < previous:
                raise AssertionError("memory check output out of order")
            previous = key
            count += 1
        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
    finally:
        import shutil

        shutil.rmtree(workdir, ignore_errors=True)

    delta = peak - baseline
    envelope = 8 * buffer_bytes + (2 << 20)
    check(count == item_count, "memory check produced all pairs")
    check(
        delta <= envelope,
        "peak allocation %d bytes <= 8*buffer+2MiB (%d bytes), %.2fx buffer"
        % (delta, envelope, delta / buffer_bytes),
    )


# ---------------------------------------------------------------------------
# F. Manifest hygiene (the runtime manifest itself)
# ---------------------------------------------------------------------------

def verify_manifest():
    manifest_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), ".pointwise-runtime.json"
    )
    try:
        with open(manifest_path, encoding="utf-8") as stream:
            manifest = json.load(stream)
    except (OSError, ValueError) as error:
        check(False, "runtime manifest is readable JSON (%s)" % error)
        return

    def is_argv(value):
        return (
            isinstance(value, list)
            and value
            and all(isinstance(part, str) and part for part in value)
        )

    check(manifest.get("version") == 1, "runtime manifest version is 1")
    check(manifest.get("kind") == "cli", "runtime manifest kind is cli")
    setup = manifest.get("setup")
    check(
        isinstance(setup, list) and all(is_argv(argv) for argv in setup),
        "runtime manifest setup is a list of argv arrays",
    )
    check(is_argv(manifest.get("verify")), "runtime manifest verify is an argv array")
    check(is_argv(manifest.get("demo")), "runtime manifest demo is an argv array")


# ---------------------------------------------------------------------------

def main():
    workdir = tempfile.mkdtemp(prefix="externaljoin-verify-")
    print("workdir:", workdir)
    try:
        print("\n[A] contract example, single pass, workdir preservation")
        verify_contract_example(workdir)

        print("\n[B] randomised reference comparison")
        verify_randomised(workdir)

        print("\n[C] hot group spill and complete Cartesian product")
        hot_dir = tempfile.mkdtemp(prefix="externaljoin-verify-hot-")
        try:
            verify_hot_group(hot_dir)
        finally:
            import shutil

            shutil.rmtree(hot_dir, ignore_errors=True)

        print("\n[D] cleanup on early close and source exceptions")
        verify_cleanup(workdir)

        print("\n[E] peak allocation envelope")
        verify_memory_envelope()

        print("\n[F] runtime manifest")
        verify_manifest()
    finally:
        import shutil

        shutil.rmtree(workdir, ignore_errors=True)

    print()
    if _failures:
        print("%d check(s) FAILED" % len(_failures))
        for failure in _failures:
            print("  - " + failure)
        return 1
    print("VERIFY OK: all external join checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
