#!/usr/bin/env python3
import argparse
import sys


def load_top_vocab(path: str, top_n: int) -> set[str]:
    vocab: set[str] = set()
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f):
            if i >= top_n:
                break
            line = line.rstrip("\n")
            if not line:
                continue
            try:
                word, _count = line.split("\t", 1)
            except ValueError:
                continue
            vocab.add(word)
    return vocab


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Отфильтровать фразы по словарю: оставить только те, "
            "в которых все слова входят в top-N слов."
        )
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="Вход: phrase<TAB>count.",
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Выход: отфильтрованный phrase<TAB>count.",
    )
    parser.add_argument(
        "--word-freq",
        required=True,
        help="Файл word<TAB>count (из step3_word_freq.py).",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=5000,
        help="Сколько самых частотных слов включить в словарь. По умолчанию 5000.",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=5,
        help="Доп. фильтр: минимальная частота фразы. По умолчанию 5.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=1_000_000,
        help="Интервал прогресса по строкам.",
    )

    args = parser.parse_args()

    vocab = load_top_vocab(args.word_freq, args.top_n)
    print(f"[info] loaded vocab of {len(vocab):,} words (top-{args.top_n})", file=sys.stderr)

    total = 0
    kept = 0
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

            try:
                phrase, count_str = line.rsplit("\t", 1)
                count = int(count_str)
            except ValueError:
                continue

            if count < args.min_count:
                continue

            words = phrase.split()
            # если ВСЕ слова из допустимого словаря — оставляем фразу
            if all(w in vocab for w in words):
                fout.write(line + "\n")
                kept += 1

    print(f"[done] total lines: {total:,}", file=sys.stderr)
    print(f"[done] kept: {kept:,}", file=sys.stderr)
    print(f"[done] removed: {total - kept:,}", file=sys.stderr)


if __name__ == "__main__":
    main()
