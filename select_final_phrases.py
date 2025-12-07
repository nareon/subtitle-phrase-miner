#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

from tqdm import tqdm


def main():
    parser = argparse.ArgumentParser(
        description="Отбор финального частотного словаря фраз из агрегированных кластеров."
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="Вход: clusters_aggregated.tsv (cluster_id,cluster_freq,cluster_size,representative).",
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Выход: final_phrases.tsv (phrase<TAB>freq<TAB>cluster_size).",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=0,
        help="Взять top-K кластеров по частоте (0 = без ограничения).",
    )
    parser.add_argument(
        "--min-freq",
        type=int,
        default=0,
        help="Минимальная суммарная частота кластера (cluster_freq). По умолчанию 0.",
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=1,
        help="Минимальный размер кластера (cluster_size). По умолчанию 1.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=500000,
        help="Интервал прогресса при чтении.",
    )

    args = parser.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)

    rows = []
    total = 0
    next_progress = args.progress_interval

    # clusters_aggregated.tsv:
    # cluster_id \t cluster_freq \t cluster_size \t representative
    with in_path.open("r", encoding="utf-8") as fin:
        header = fin.readline()  # пропускаем заголовок
        for line in fin:
            total += 1
            if total >= next_progress:
                print(f"[read] {total:,} lines...", file=sys.stderr)
                next_progress += args.progress_interval

            line = line.rstrip("\n")
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) != 4:
                continue

            cid_str, freq_str, size_str, phrase = parts
            try:
                freq = int(freq_str)
                size = int(size_str)
            except ValueError:
                continue

            # фильтры по частоте и размеру кластера
            if freq < args.min_freq:
                continue
            if size < args.min_size:
                continue

            rows.append((phrase, freq, size))

    print(f"[info] total clusters read: {total:,}", file=sys.stderr)
    print(f"[info] clusters after filters: {len(rows):,}", file=sys.stderr)

    # сортировка по частоте (убывание)
    rows.sort(key=lambda x: x[1], reverse=True)

    if args.top_k > 0 and len(rows) > args.top_k:
        rows = rows[: args.top_k]
        print(f"[info] taking top-{args.top_k} clusters", file=sys.stderr)

    # запись финального словаря
    with out_path.open("w", encoding="utf-8") as fout:
        # без заголовка, чтобы удобно было дальше обрабатывать
        for phrase, freq, size in rows:
            fout.write(f"{phrase}\t{freq}\t{size}\n")

    print(f"[done] written {len(rows):,} phrases to {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
