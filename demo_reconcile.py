#!/usr/bin/env python3
"""Realistic reconciliation demo for :func:`externaljoin.join`.

Two independent ledger exports are joined on an account id.  The left export
contains the bank's settled entries, the right export contains the processor's
captured entries.  Duplicated keys become Cartesian pairs; a key present on
one side only becomes an ``unmatched`` line.

Run with ``--quiet`` to print a summary only.
"""

import argparse
import os
import random
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from externaljoin import join  # noqa: E402

ACCOUNTS = 40_000
DEFAULT_LEDGER_ROWS = 250_000
DEFAULT_PROCESSOR_ROWS = 230_000


def make_ledger(seed, row_count):
    """Settled entries; accounts 0..N.  Some accounts repeat (batch splits)."""
    rng = random.Random(seed)
    for index in range(row_count):
        account = rng.randrange(ACCOUNTS)
        if rng.random() < 0.004:  # a few genuinely hot accounts
            account = rng.randrange(8)
        yield account, "L-%s-%06d-%s" % (
            "hot" if account < 8 else "acct",
            index,
            "%016x" % rng.getrandbits(64),
        )


def make_processor(seed, row_count):
    """Captured entries: covers most ledger accounts, misses some, has strays."""
    rng = random.Random(seed)
    for index in range(row_count):
        account = rng.randrange(ACCOUNTS + 500)
        if rng.random() < 0.004:
            account = rng.randrange(8)
        yield account, "P-%06d-%s" % (index, "%016x" % rng.getrandbits(64))


def run(ledger, processor, workdir, buffer_bytes, verbose=True):
    started = time.monotonic()
    matched = 0
    ledger_only = 0
    processor_only = 0
    hot_pairs = 0
    hot_keys = set()
    previous_key = None
    first_hot = []
    files_during = 0

    for key, left_payload, right_payload in join(
        ledger, processor, workdir, buffer_bytes
    ):
        if previous_key is not None and key < previous_key:
            raise RuntimeError("join output is not key-ascending")
        previous_key = key

        if left_payload is not None and right_payload is not None:
            matched += 1
            if len(first_hot) < 5 and key < 8:
                first_hot.append((key, left_payload, right_payload))
        elif left_payload is not None:
            ledger_only += 1
        else:
            processor_only += 1

        if key < 8:
            hot_keys.add(key)
            hot_pairs += 1

        if verbose and (matched + ledger_only + processor_only) % 100_000 == 0:
            try:
                files_during = max(files_during, len(os.listdir(workdir)))
            except OSError:
                pass

    elapsed = time.monotonic() - started
    return {
        "matched_pairs": matched,
        "ledger_only": ledger_only,
        "processor_only": processor_only,
        "hot_accounts": sorted(hot_keys),
        "hot_pairs": hot_pairs,
        "elapsed_seconds": round(elapsed, 2),
        "sample_hot_pairs": first_hot,
        "temp_files_seen": files_during,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quiet", action="store_true", help="summary only")
    parser.add_argument(
        "--buffer-bytes", type=int, default=1 << 20, help="in-memory run budget"
    )
    parser.add_argument("--ledger-rows", type=int, default=DEFAULT_LEDGER_ROWS)
    parser.add_argument("--processor-rows", type=int, default=DEFAULT_PROCESSOR_ROWS)
    args = parser.parse_args(argv)

    workdir = tempfile.mkdtemp(prefix="externaljoin-demo-")
    try:
        report = run(
            make_ledger(11, args.ledger_rows),
            make_processor(22, args.processor_rows),
            workdir,
            args.buffer_bytes,
            verbose=not args.quiet,
        )
        leftover = [
            name for name in os.listdir(workdir) if name.startswith(".externaljoin-")
        ]
        report["workdir_leftovers"] = leftover
    finally:
        import shutil

        shutil.rmtree(workdir, ignore_errors=True)

    print("reconciliation: ledger x processor")
    print("  matched pairs      :", report["matched_pairs"])
    print("  ledger-only rows   :", report["ledger_only"])
    print("  processor-only rows:", report["processor_only"])
    print("  hot accounts       :", report["hot_accounts"])
    print("  hot account pairs  :", report["hot_pairs"])
    print("  elapsed seconds    :", report["elapsed_seconds"])
    print("  temp leftovers     :", report["workdir_leftovers"])
    if not args.quiet:
        print("  sample hot pairs   :")
        for key, left, right in report["sample_hot_pairs"]:
            print("    #%d  %s  <->  %s" % (key, left, right))
    return 0


if __name__ == "__main__":
    sys.exit(main())
