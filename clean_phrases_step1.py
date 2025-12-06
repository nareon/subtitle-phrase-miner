#!/usr/bin/env python3
import argparse
import os
import sys
import re
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat
from typing import List, Iterable


# =========================
#   REGEX ДЛЯ ПРЕ-ОЧИСТКИ
# =========================

# URL: http(s)://... или www....
URL_RE = re.compile(
    r"(https?://\S+)|(www\.\S+)",
    flags=re.IGNORECASE,
)

# Теги субтитров в [] <> {} (), музыкальные ♪…♪
BRACKETS_RE = re.compile(
    r"\[[^\]]*\]"      # [ ... ]
    r"|<[^>]*>"        # < ... >
    r"|\{[^}]*\}"      # { ... }
    r"|\([^)]*\)"      # ( ... )
    r"|♪[^♪]*♪",       # ♪ ... ♪
    flags=re.UNICODE,
)


def strip_tags_and_urls(line: str) -> str:
    """
    Удаляет:
      - гиперссылки (http(s)://..., www....)
      - теги субтитров: [..], <..>, {..}, (..), ♪..♪
    Заменяет их пробелами (чтобы не склеивать слова).
    """
    line = URL_RE.sub(" ", line)
    line = BRACKETS_RE.sub(" ", line)
    return line


def clean_line(line: str, min_words: int, max_words: int) -> str | None:
    """
    Грубая очистка одной строки:
      1) убираем теги субтитров и URL
      2) приводим к нижнему регистру
      3) оставляем только буквы и пробелы
      4) схлопываем пробелы, режем по краям
      5) оставляем фразы длиной от min_words до max_words
    """
    # Шаг 1: теги и URL
    line = strip_tags_and_urls(line)

    # Шаг 2: lower + trim
    line = line.strip().lower()
    if not line:
        return None

    # Шаг 3: только буквы и пробелы (прочее -> пробел)
    chars = []
    for ch in line:
        if ch.isalpha():
            chars.append(ch)
        else:
            chars.append(" ")
    cleaned = "".join(chars)

    # Шаг 4: токенизация и фильтр по длине
    tokens = cleaned.split()
    n = len(tokens)
    if min_words <= n <= max_words:
        return " ".join(tokens)
    return None


def process_chunk(
    lines: List[str],
    min_words: int,
    max_words: int,
) -> List[str]:
    """
    Обработка чанка строк. Возвращает список очищенных строк
    (без пустых и слишком коротких/длинных).
    """
    out_lines: List[str] = []
    for line in lines:
        r = clean_line(line, min_words, max_words)
        if r is not None:
            out_lines.append(r + "\n")
    return out_lines


def chunk_reader(
    fh,
    chunk_size: int,
) -> Iterable[List[str]]:
    """
    Читает файл чанками по chunk_size строк.
    """
    chunk: List[str] = []
    for line in fh:
        chunk.append(line)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def main():
    parser = argparse.ArgumentParser(
        description="Шаг 1: грубая очистка корпуса фраз (2–6 слов, только буквы, lower-case, без URL и тегов)."
    )
    parser.add_argument(
        "-i", "--input",
        type=str,
        required=True,
        help="Входной файл (одна фраза на строку).",
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        required=True,
        help="Файл для записи очищенных фраз.",
    )
    parser.add_argument(
        "--min-words",
        type=int,
        default=2,
        help="Мин. число слов в фразе (включительно). По умолчанию 2.",
    )
    parser.add_argument(
        "--max-words",
        type=int,
        default=6,
        help="Макс. число слов в фразе (включительно). По умолчанию 6.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100_000,
        help="Число строк в одном чанке для параллельной обработки. По умолчанию 100000.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=os.cpu_count() or 1,
        help="Число процессов-воркеров. По умолчанию = числу CPU.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=1_000_000,
        help="Как часто показывать прогресс (по числу обработанных строк). По умолчанию 1_000_000.",
    )

    args = parser.parse_args()

    total_lines = 0

    with open(args.input, "r", encoding="utf-8", errors="ignore") as fin, \
         open(args.output, "w", encoding="utf-8") as fout, \
         ProcessPoolExecutor(max_workers=args.workers) as ex:

        chunks = list(chunk_reader(fin, args.chunk_size))

        # Чтобы прогресс был точным, считаем реальный размер каждого чанка
        total_input_lines = sum(len(c) for c in chunks)
        print(f"[info] total input lines: {total_input_lines:,}", file=sys.stderr)

        for out_lines, chunk in zip(
            ex.map(
                process_chunk,
                chunks,
                repeat(args.min_words),
                repeat(args.max_words),
            ),
            chunks,
        ):
            fout.writelines(out_lines)
            total_lines += len(chunk)

            if total_lines % args.progress_interval < len(chunk):
                print(f"[progress] processed {total_lines:,} / {total_input_lines:,} lines",
                      file=sys.stderr)

    print(f"[done] total processed lines: {total_lines:,}", file=sys.stderr)


if __name__ == "__main__":
    main()
