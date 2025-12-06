#!/usr/bin/env python3
import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Удалить фразы, у которых количество < MIN_COUNT."
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="Входной файл частотного словаря: phrase<TAB>count",
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Файл для записи отфильтрованного словаря.",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=5,
        help="Минимальное количество вхождений. По умолчанию 5.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=1_000_000,
        help="Как часто показывать прогресс по строкам.",
    )

    args = parser.parse_args()

    kept = 0
    total = 0
    next_progress = args.progress_interval

    with open(args.input, "r", encoding="utf-8", errors="ignore") as fin, \
         open(args.output, "w", encoding="utf-8") as fout:

        for line in fin:
            total += 1

            if total >= next_progress:
                print(f"[progress] processed {total:,} lines, kept {kept:,}", file=sys.stderr)
                next_progress += args.progress_interval

            line = line.rstrip("\n")
            if not line:
                continue

            # ожидаем формат: фраза<TAB>count
            try:
                phrase, count_str = line.rsplit("\t", 1)
                count = int(count_str)
            except ValueError:
                # пропускаем повреждённые строки
                continue

            if count >= args.min_count:
                fout.write(line + "\n")
                kept += 1

    print(f"[done] total lines processed: {total:,}", file=sys.stderr)
    print(f"[done] kept phrases: {kept:,}", file=sys.stderr)
    print(f"[done] removed: {total - kept:,}", file=sys.stderr)


if __name__ == "__main__":
    main()
