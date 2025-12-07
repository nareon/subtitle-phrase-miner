#!/usr/bin/env python3
import sys
import argparse
from pathlib import Path

# Однословные вопросительные слова (с ударением)
QUESTION_WORDS_1 = {
    "qué", "quién", "quiénes",
    "cuál", "cuáles",
    "cuánto", "cuánta", "cuántos", "cuántas",
    "dónde", "cuándo", "cómo",
    "adónde",
}

# Двухсловные устойчивые начала вопросов
QUESTION_WORDS_2 = {
    ("por", "qué"),
    ("para", "qué"),
    ("a", "qué"),
    ("de", "qué"),
    ("con", "qué"),
    ("a", "quién"),
    ("de", "quién"),
    ("por", "quién"),
    ("para", "quién"),
    ("con", "quién"),
    # при необходимости можно расширить
}


def is_strong_question(phrase: str) -> bool:
    """
    Эвристика: фраза явно вопросительная по грамматике?
    Смотрим только первые 1–2 слова.
    """
    words = phrase.split()
    if not words:
        return False

    w1 = words[0]
    if w1 in QUESTION_WORDS_1:
        return True

    if len(words) >= 2:
        w2 = words[1]
        if (w1, w2) in QUESTION_WORDS_2:
            return True

    return False


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Восстановление знаков вопроса ¿ ? для явно вопросительных фраз "
            "в корпусе phrase<TAB>freq<TAB>cluster_size."
        )
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="Входной файл корпуса (например, final_phrases_top300k.tsv).",
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Выходной файл с восстановленными знаками вопроса.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=200_000,
        help="Интервал прогресса по числу строк (по умолчанию 200000).",
    )

    args = parser.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)

    n_total = 0
    n_q = 0

    with in_path.open("r", encoding="utf-8") as fin, \
         out_path.open("w", encoding="utf-8") as fout:

        for line in fin:
            n_total += 1
            if n_total % args.progress_interval == 0:
                print(f"[progress] {n_total:,} lines processed, "
                      f"{n_q:,} marked as questions", file=sys.stderr)

            line = line.rstrip("\n")
            if not line:
                continue

            parts = line.split("\t")
            phrase = parts[0]

            if is_strong_question(phrase):
                # восстановление знаков вопроса
                phrase_q = f"¿{phrase}?"
                parts[0] = phrase_q
                n_q += 1

            fout.write("\t".join(parts) + "\n")

    print(f"[done] total lines: {n_total:,}", file=sys.stderr)
    print(f"[done] questions marked: {n_q:,}", file=sys.stderr)
    if n_total > 0:
        print(f"[done] percentage: {n_q / n_total * 100:.2f} %", file=sys.stderr)


if __name__ == "__main__":
    main()
