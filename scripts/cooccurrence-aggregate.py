#!/usr/bin/env python3
"""Build the exact H0 baseline offline using bounded NumPy partitions.

Usage: python scripts/cooccurrence-aggregate.py ABSOLUTE_ARTIFACT_DIRECTORY
No network access. Registry records and comments are never written to source control.
"""
import base64
import json
import math
import sys
from collections import Counter
from pathlib import Path

import numpy as np

root = Path(sys.argv[1]).resolve()
if root == Path.cwd() or Path.cwd() in root.parents:
    raise ValueError("Artifacts must be outside the repository")
manifest = json.loads((root / "snapshot.json").read_text(encoding="utf-8"))
if not manifest.get("scanComplete"):
    raise ValueError("Complete the scan first")


def subject_set(raw):
    values = json.loads(raw)
    if not isinstance(values, list) or len(values) > 9:
        raise ValueError("Invalid subject set")
    if any(value is not None and (not isinstance(value, str) or not value.strip()) for value in values):
        raise ValueError("Invalid external subject ID")
    return sorted({value.strip() for value in values if value is not None})


# The first event for each affected share reconstructs its state at H0,
# regardless of when that share was observed by the nonblocking live scan.
corrections = {}
for file in sorted((root / "events").glob("*.json")):
    for event in json.loads(file.read_text(encoding="utf-8")):
        old, new = event["old_share_id"], event["new_share_id"]
        if old is not None and old not in corrections:
            corrections[old] = (event["old_kind"], subject_set(event["old_ids"]))
        if new is not None and new != old and new not in corrections:
            corrections[new] = None


def shares():
    for file in sorted((root / "pages").glob("*.json")):
        for row in json.loads(file.read_text(encoding="utf-8")):
            if row["share_id"] not in corrections:
                yield row["kind"], subject_set(row["ids"])
    for value in corrections.values():
        if value is not None:
            yield value


frequency = Counter()
share_count = 0
contributions = 0
for kind, subjects in shares():
    share_count += 1
    contributions += len(subjects) * (len(subjects) - 1)
    frequency.update((kind, subject) for subject in subjects)
identities = sorted(frequency)
ids = {key: index + 1 for index, key in enumerate(identities)}
print(json.dumps({"shares": share_count, "subjects": len(ids), "contributions": contributions}), flush=True)
if len(ids) > 0xffffffff or max(frequency.values(), default=0) > 0xffffffff:
    raise ValueError("Requires uint64 schema upgrade")

partition_size = 4096
partition_dir = root / "partitions"
partition_dir.mkdir(exist_ok=True)
files = [open(partition_dir / f"{i:04}.bin", "wb") for i in range(math.ceil(len(ids) / partition_size))]
buffers = [[] for _ in files]
try:
    for index, (kind, subjects) in enumerate(shares()):
        members = [ids[kind, subject] for subject in subjects]
        for source in members:
            buffer = buffers[(source - 1) // partition_size]
            buffer.extend((source << 32) | peer for peer in members if peer != source)
        if (index + 1) % 4000 == 0:
            for file, buffer in zip(files, buffers):
                np.asarray(buffer, dtype="<u8").tofile(file)
                buffer.clear()
    for file, buffer in zip(files, buffers):
        np.asarray(buffer, dtype="<u8").tofile(file)
finally:
    for file in files:
        file.close()

out = root / "import"
out.mkdir(exist_ok=True)
if any(out.iterdir()):
    raise ValueError("Import artifacts already exist; use the completed aggregate or a fresh output directory")
batch = []
batch_bytes = 0
batch_number = 0
previous = 0
total_bytes = 0
max_bytes = 0


def flush():
    global batch, batch_bytes, batch_number, previous
    if not batch:
        return
    payload = {"baseline": manifest["h0"], "updatedAt": manifest["at"], "previous": previous, "rows": batch}
    (out / f"{batch_number:06}.json").write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    previous = batch[-1]["id"]
    batch_number += 1
    batch = []
    batch_bytes = 0


for partition in range(len(files)):
    raw = np.fromfile(partition_dir / f"{partition:04}.bin", dtype="<u8")
    keys, counts = np.unique(raw, return_counts=True)
    del raw
    sources = keys >> np.uint64(32)
    for source in range(partition * partition_size + 1, min((partition + 1) * partition_size, len(ids)) + 1):
        # Match uint64 explicitly; a Python signed int can coerce/copy the
        # entire uint64 partition to float64 on every binary search.
        start = int(np.searchsorted(sources, np.uint64(source), side="left"))
        end = int(np.searchsorted(sources, np.uint64(source), side="right"))
        vector = np.empty((end - start, 2), dtype="<u4")
        vector[:, 0] = keys[start:end] & np.uint64(0xffffffff)
        if end > start and int(counts[start:end].max()) > 0xffffffff:
            raise ValueError("Counter overflow")
        vector[:, 1] = counts[start:end]
        packed = vector.tobytes()
        if len(packed) > 1_900_000:
            raise ValueError("D1 vector row limit")
        if len(batch) >= 100 or batch_bytes + len(packed) > 1_048_576:
            flush()
        kind, subject = identities[source - 1]
        batch.append({"id": source, "kind": kind, "subject_id": subject,
                      "matched": frequency[kind, subject], "counts": base64.b64encode(packed).decode("ascii")})
        batch_bytes += len(packed)
        total_bytes += len(packed)
        max_bytes = max(max_bytes, len(packed))
    print(json.dumps({"partition": partition + 1, "bytes": total_bytes, "batches": batch_number}), flush=True)
flush()
stats = {"baseline": manifest["h0"], "shares": share_count, "subjects": len(ids), "matched": sum(frequency.values()),
         "contributions": contributions, "bytes": total_bytes, "maxBytes": max_bytes, "batches": batch_number}
(root / "aggregate.json").write_text(json.dumps(stats), encoding="utf-8")
print(json.dumps(stats), flush=True)
