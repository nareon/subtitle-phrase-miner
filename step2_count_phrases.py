#!/usr/bin/env python3
import argparse
import os
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Iterable


def chunk_reader(fh, chunk_size: int) -> Iterable[List[str]]:
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


def count_chunk(lines: List[str]) -> Counter:
    """
    Считает частоты фраз в одном чанке.
    Фразы уже очищены на предыдущем шаге.
    """
    c = Counter()
    for line in lines:
        phrase = line.strip()
        if phrase:
            c[phrase] += 1
    return c


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Шаг 2: посчитать частоты фраз, "
            "отсортировать по убыванию и удалить хвост самых редких."
        )
    )
    parser.add_argument(
        "-i", "--input",
        type=str,
        required=True,
        help="Входной файл очищенных фраз (по одной фразе на строку).",
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        required=True,
        help="Файл для записи частотного словаря (phrase<TAB>count).",
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
    parser.add_argument(
        "--tail-percent",
        type=float,
        default=5.0,
        help="Процент самых редких фраз, которые нужно удалить (по количеству типов). По умолчанию 5.0.",
    )

    args = parser.parse_args()

    global_counter: Counter = Counter()
    total_lines = 0

    print(f"[info] counting frequencies from {args.input}", file=sys.stderr)
    print(f"[info] using {args.workers} workers, chunk_size={args.chunk_size}", file=sys.stderr)

    with open(args.input, "r", encoding="utf-8", errors="ignore") as fin, \
         ProcessPoolExecutor(max_workers=args.workers) as ex:

        future_to_len = {}
        for chunk in chunk_reader(fin, args.chunk_size):
            fut = ex.submit(count_chunk, chunk)
            future_to_len[fut] = len(chunk)

        next_progress = args.progress_interval

        for fut in as_completed(future_to_len):
            c = fut.result()
            global_counter.update(c)
            total_lines += future_to_len[fut]

            if total_lines >= next_progress:
                print(f"[progress] processed {total_lines:,} lines", file=sys.stderr)
                next_progress += args.progress_interval

    vocab_size = len(global_counter)
    print(f"[info] total lines processed: {total_lines:,}", file=sys.stderr)
    print(f"[info] vocabulary size before trimming: {vocab_size:,}", file=sys.stderr)

    # Удаляем хвост tail-percent самых редких фраз
    tail_percent = max(0.0, min(100.0, args.tail_percent))
    items = list(global_counter.items())

    if tail_percent > 0.0 and vocab_size > 0:
        items.sort(key=lambda x: x[1])  # по возрастанию частоты
        tail_n = int(vocab_size * (tail_percent / 100.0))
        if tail_n > 0:
            items = items[tail_n:]
        print(
            f"[info] removed tail {tail_percent:.2f}% "
            f"({tail_n:,} phrase types), kept {len(items):,}",
            file=sys.stderr,
        )
    else:
        print("[info] tail trimming disabled", file=sys.stderr)

    # Сортируем по убыванию частоты
    items.sort(key=lambda x: x[1], reverse=True)

    with open(args.output, "w", encoding="utf-8") as fout:
        for phrase, count in items:
            fout.write(f"{phrase}\t{count}\n")

    print(f"[done] written {len(items):,} phrases to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
