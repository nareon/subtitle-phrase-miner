#!/usr/bin/env python3
import argparse
import sys
from collections import Counter


def main():
    parser = argparse.ArgumentParser(
        description="Построить частотный словарь слов из файла phrase<TAB>count."
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="Вход: файл с фразами и частотами (phrase<TAB>count).",
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Выход: файл слов и частот (word<TAB>count), отсортированный по убыванию.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=1_000_000,
        help="Интервал прогресса по строкам. По умолчанию 1_000_000.",
    )
    args = parser.parse_args()

    word_freq = Counter()
    total_lines = 0
    next_progress = args.progress_interval

    with open(args.input, "r", encoding="utf-8", errors="ignore") as fin:
        for line in fin:
            total_lines += 1
            if total_lines >= next_progress:
                print(f"[progress] processed {total_lines:,} lines", file=sys.stderr)
                next_progress += args.progress_interval

            line = line.rstrip("\n")
            if not line:
                continue

            try:
                phrase, count_str = line.rsplit("\t", 1)
                count = int(count_str)
            except ValueError:
                continue

            words = phrase.split()
            for w in words:
                if w:
                    word_freq[w] += count

    print(f"[info] total lines processed: {total_lines:,}", file=sys.stderr)
    print(f"[info] vocab size: {len(word_freq):,}", file=sys.stderr)

    # сортировка по убыванию частоты
    items = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)

    with open(args.output, "w", encoding="utf-8") as fout:
        for w, c in items:
            fout.write(f"{w}\t{c}\n")

    print(f"[done] written {len(items):,} words to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
