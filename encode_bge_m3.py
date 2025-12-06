#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

import numpy as np
import torch
from sentence_transformers import SentenceTransformer
from tqdm import tqdm


def count_lines(path: Path, progress_step=1_000_000):
    total = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for _ in f:
            total += 1
            if total % progress_step == 0:
                print(f"[count] {total:,}...", file=sys.stderr)
    print(f"[count] total lines = {total:,}", file=sys.stderr)
    return total


def main():
    parser = argparse.ArgumentParser(
        description="Encode phrases using BGE-M3 (1024-dim, fp16)"
    )
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-d", "--out-dir", required=True)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--max-lines", type=int, default=0)
    args = parser.parse_args()

    in_path = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---------------------------
    # 1. Count phrases (по строкам файла)
    # ---------------------------
    total_lines = count_lines(in_path)
    if args.max_lines > 0 and args.max_lines < total_lines:
        total_lines = args.max_lines
        print(f"[info] truncating to {total_lines:,} lines", file=sys.stderr)

    # ---------------------------
    # 2. Load model
    # ---------------------------
    print("[info] loading BGE-M3...", file=sys.stderr)
    model = SentenceTransformer("BAAI/bge-m3")
    model = model.to("cuda")
    dim = model.get_sentence_embedding_dimension()
    print(f"[info] embedding dim = {dim}", file=sys.stderr)

    # ---------------------------
    # 3. Prepare memmap
    # ---------------------------
    emb_path = out_dir / "bge_m3_embeddings.dat"
    meta_path = out_dir / "bge_m3_meta.tsv"

    # резервируем по числу строк файла (может оказаться чуть с запасом,
    # если какие-то строки будут пропущены)
    emb = np.memmap(
        emb_path, dtype="float16", mode="w+", shape=(total_lines, dim)
    )

    # ---------------------------
    # 4. Streaming encoding FP16
    # ---------------------------
    line_idx = 0          # сколько строк файла прочитали
    row = 0               # сколько фраз реально закодировали
    batch_texts = []
    batch_meta = []

    with in_path.open("r", encoding="utf-8", errors="ignore") as fin, \
            meta_path.open("w", encoding="utf-8") as fmeta, \
            torch.autocast("cuda", dtype=torch.float16):

        progress = tqdm(total=total_lines, desc="encoding", unit="line")

        for line in fin:
            if line_idx >= total_lines:
                break
            line_idx += 1
            progress.update(1)

            line = line.rstrip("\n")
            if not line:
                continue

            # file is of format "phrase<TAB>count"
            try:
                phrase, count_str = line.rsplit("\t", 1)
                freq = int(count_str)
            except ValueError:
                # битая строка — пропускаем, но line_idx уже учтён
                continue

            batch_texts.append(phrase)
            batch_meta.append((phrase, freq, len(phrase.split())))

            if len(batch_texts) >= args.batch_size:
                vectors = model.encode(
                    batch_texts,
                    batch_size=args.batch_size,
                    convert_to_numpy=True,
                    normalize_embeddings=True,
                ).astype("float16")

                n = vectors.shape[0]
                emb[row:row + n, :] = vectors

                for phr, fr, ln in batch_meta:
                    fmeta.write(f"{row}\t{phr}\t{fr}\t{ln}\n")
                    row += 1

                batch_texts.clear()
                batch_meta.clear()

        # Final incomplete batch
        if batch_texts:
            vectors = model.encode(
                batch_texts,
                batch_size=len(batch_texts),
                convert_to_numpy=True,
                normalize_embeddings=True,
            ).astype("float16")

            n = vectors.shape[0]
            emb[row:row + n, :] = vectors
            for phr, fr, ln in batch_meta:
                fmeta.write(f"{row}\t{phr}\t{fr}\t{ln}\n")
                row += 1

        progress.close()

    del emb
    print(f"[done] embeddings saved to {emb_path}", file=sys.stderr)
    print(f"[done] meta saved to      {meta_path}", file=sys.stderr)
    print(f"[done] encoded rows:      {row:,}", file=sys.stderr)
    print(f"[info] total lines read:  {line_idx:,}", file=sys.stderr)


if __name__ == "__main__":
    main()
