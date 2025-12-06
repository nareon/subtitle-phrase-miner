#!/usr/bin/env python3
import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Подсчёт количества фраз длиной 2,3,4,5,6 слов в корпусе."
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="Файл фраз: phrase<TAB>count или просто phrase.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=500000,
        help="Прогресс каждые N строк.",
    )
    args = parser.parse_args()

    counts = {2: 0, 3: 0, 4: 0, 5: 0, 6: 0}
    total = 0
    next_progress = args.progress_interval

    with open(args.input, "r", encoding="utf-8", errors="ignore") as fin:
        for line in fin:
            total += 1
            if total >= next_progress:
                print(f"[progress] {total:,} lines processed", file=sys.stderr)
                next_progress += args.progress_interval

            line = line.rstrip("\n")

            if "\t" in line:
                phrase, _freq = line.rsplit("\t", 1)
            else:
                phrase = line

            words = phrase.split()
            n = len(words)
            if n in counts:
                counts[n] += 1

    print("\n=== RESULT ===")
    print(f"Total phrases: {total:,}\n")
    for n in range(2, 7):
        print(f"{n} words : {counts[n]:,}")
    print("\nPercentages:")
    for n in range(2, 7):
        pct = (counts[n] / total) * 100 if total else 0
        print(f"{n} words : {pct:.2f} %")


if __name__ == "__main__":
    main()
