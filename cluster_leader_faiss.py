#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

import numpy as np
import faiss
from tqdm import tqdm


def load_memmap(path: Path, dim: int, dtype="float16") -> np.memmap:
    # shape: (N, dim); N нужно вычислить по размеру файла
    bytes_per = np.dtype(dtype).itemsize
    n = path.stat().st_size // (bytes_per * dim)
    print(f"[info] memmap: {n:,} x {dim} ({dtype})", file=sys.stderr)
    arr = np.memmap(path, mode="r", dtype=dtype, shape=(n, dim))
    return arr


def main():
    parser = argparse.ArgumentParser(
        description="Leader clustering on BGE-M3 embeddings using FAISS (cosine)."
    )
    parser.add_argument("--emb", required=True, help="bge_m3_embeddings.dat")
    parser.add_argument("--dim", type=int, default=1024, help="Embedding dimension.")
    parser.add_argument("--out", required=True, help="Output: cluster_id per line.")
    parser.add_argument("--k", type=int, default=32, help="K nearest neighbors to check.")
    parser.add_argument("--threshold", type=float, default=0.92,
                        help="Cosine similarity threshold.")
    parser.add_argument("--progress-interval", type=int, default=10000)
    args = parser.parse_args()

    emb_path = Path(args.emb)
    emb = load_memmap(emb_path, args.dim, dtype="float16")
    n, d = emb.shape

    # FAISS работает с float32
    print("[info] building FAISS index (HNSW)...", file=sys.stderr)
    index = faiss.IndexHNSWFlat(d, 32, faiss.METRIC_INNER_PRODUCT)
    index.hnsw.efConstruction = 200
    index.verbose = True

    # Нормировка уже сделана при encode, но если сомневаемся, можно повторно
    emb32 = np.array(emb, dtype="float32")
    index.add(emb32)

    print("[info] index built, starting leader clustering...", file=sys.stderr)

    cluster_id = np.full(n, -1, dtype=np.int32)
    current_cluster = 0

    with tqdm(total=n, desc="clustering", unit="phr") as pbar:
        for i in range(n):
            if cluster_id[i] != -1:
                pbar.update(1)
                continue

            # Новый кластер
            cid = current_cluster
            current_cluster += 1
            cluster_id[i] = cid

            # NN search
            x = emb32[i : i + 1]
            D, I = index.search(x, args.k)  # D: (1, k) similarities, I: (1, k) indices
            sims = D[0]
            neigh = I[0]

            for sim, j in zip(sims[1:], neigh[1:]):  # [0] — это i само
                if j < 0:
                    continue
                if sim < args.threshold:
                    continue
                if cluster_id[j] == -1:
                    cluster_id[j] = cid

            pbar.update(1)

    print(f"[info] total clusters: {current_cluster:,}", file=sys.stderr)

    out_path = Path(args.out)
    np.savetxt(out_path, cluster_id, fmt="%d")
    print(f"[done] cluster ids written to {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
