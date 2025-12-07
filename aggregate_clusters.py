#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path
from collections import defaultdict

import numpy as np
from tqdm import tqdm


def main():
    parser = argparse.ArgumentParser(
        description="Aggregate clusters: compute cluster freq, size, and select representative phrase."
    )
    parser.add_argument("--meta", required=True, help="bge_m3_meta.tsv")
    parser.add_argument("--clusters", required=True, help="cluster_ids.txt")
    parser.add_argument("--out", required=True, help="Output CSV/TSV with aggregated clusters.")
    parser.add_argument("--progress-interval", type=int, default=500000)
    args = parser.parse_args()

    meta_path = Path(args.meta)
    cl_path = Path(args.clusters)

    print("[info] loading cluster ids...", file=sys.stderr)
    cluster_ids = np.loadtxt(cl_path, dtype=np.int32)
    n = len(cluster_ids)
    print(f"[info] total phrases: {n:,}", file=sys.stderr)

    clusters = defaultdict(list)

    print("[info] reading metadata and grouping...", file=sys.stderr)
    with meta_path.open("r", encoding="utf-8") as f:
        for idx, line in tqdm(enumerate(f), total=n):
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 4:
                continue
            pid, phrase, freq, length = parts
            pid = int(pid)
            freq = int(freq)
            length = int(length)

            cid = cluster_ids[pid]
            clusters[cid].append((phrase, freq, length))

    print("[info] computing representatives...", file=sys.stderr)

    out_path = Path(args.out)
    with out_path.open("w", encoding="utf-8") as fout:
        fout.write("cluster_id\tcluster_freq\tcluster_size\trepresentative\n")

        for cid, items in clusters.items():
            # cluster frequency
            total_freq = sum(fr for _, fr, _ in items)
            size = len(items)

            # choose representative:
            # 1) max freq
            # 2) tie → choose shorter phrase (but >= 3 words)
            items_sorted = sorted(
                items,
                key=lambda x: (-x[1], abs(x[2] - 4))  # freq desc, length close to 4
            )
            rep_phrase = items_sorted[0][0]

            fout.write(f"{cid}\t{total_freq}\t{size}\t{rep_phrase}\n")

    print(f"[done] written: {out_path}")


if __name__ == "__main__":
    main()
