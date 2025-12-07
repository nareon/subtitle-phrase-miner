#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path
from collections import Counter


def main():
    parser = argparse.ArgumentParser(
        description="Построить индексы слов и фраз из final_phrases.tsv."
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="Вход: final_phrases.tsv (phrase<TAB>freq<TAB>cluster_size).",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Каталог для файлов words.tsv, phrases.tsv, phrase_words.tsv.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=200000,
        help="Интервал прогресса по строкам.",
    )
    args = parser.parse_args()

    in_path = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Первый проход: считаем частоты слов (взвешенные freq фразы)
    word_freq = Counter()
    total_phrases = 0
    next_progress = args.progress_interval

    print(f"[info] pass 1: counting word frequencies from {in_path}", file=sys.stderr)
    with in_path.open("r", encoding="utf-8") as fin:
        for line in fin:
            total_phrases += 1
            if total_phrases >= next_progress:
                print(f"[pass1] {total_phrases:,} phrases...", file=sys.stderr)
                next_progress += args.progress_interval

            line = line.rstrip("\n")
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) < 2:
                continue

            phrase = parts[0]
            try:
                freq = int(parts[1])
            except ValueError:
                continue

            words = phrase.split()
            for w in words:
                if w:
                    word_freq[w] += freq

    print(f"[info] total phrases read: {total_phrases:,}", file=sys.stderr)
    print(f"[info] vocab size: {len(word_freq):,}", file=sys.stderr)

    # 2. Строим словарь: word -> word_id, сортируем по убыванию freq
    print("[info] building word index...", file=sys.stderr)
    words_sorted = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)

    word2id = {}
    words_path = out_dir / "words.tsv"
    with words_path.open("w", encoding="utf-8") as fout:
        fout.write("word_id\tword\ttotal_freq\trank\n")
        for rank, (w, f) in enumerate(words_sorted, start=1):
            wid = rank - 1  # можно от 0
            word2id[w] = wid
            fout.write(f"{wid}\t{w}\t{f}\t{rank}\n")

    print(f"[done] words.tsv written: {len(word2id):,} words", file=sys.stderr)

    # 3. Второй проход: phrases.tsv и phrase_words.tsv
    phrases_path = out_dir / "phrases.tsv"
    pw_path = out_dir / "phrase_words.tsv"

    print("[info] pass 2: writing phrases and phrase_words...", file=sys.stderr)
    total_phrases = 0
    next_progress = args.progress_interval

    with in_path.open("r", encoding="utf-8") as fin, \
         phrases_path.open("w", encoding="utf-8") as fphr, \
         pw_path.open("w", encoding="utf-8") as fpw:

        fphr.write("phrase_id\tphrase\tfreq\tcluster_size\tlength\n")
        # phrase_words.tsv без заголовка: phrase_id<TAB>word_id

        for phrase_id, line in enumerate(fin):
            total_phrases += 1
            if total_phrases >= next_progress:
                print(f"[pass2] {total_phrases:,} phrases...", file=sys.stderr)
                next_progress += args.progress_interval

            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue

            phrase = parts[0]
            try:
                freq = int(parts[1])
            except ValueError:
                continue

            cluster_size = 1
            if len(parts) >= 3:
                try:
                    cluster_size = int(parts[2])
                except ValueError:
                    pass

            words = phrase.split()
            length = len(words)

            # пишем фразу
            fphr.write(f"{phrase_id}\t{phrase}\t{freq}\t{cluster_size}\t{length}\n")

            # связи фраза-слово
            for w in words:
                wid = word2id.get(w)
                if wid is not None:
                    fpw.write(f"{phrase_id}\t{wid}\n")

    print(f"[done] phrases.tsv written, total phrases: {total_phrases:,}", file=sys.stderr)
    print(f"[done] phrase_words.tsv written", file=sys.stderr)


if __name__ == "__main__":
    main()
