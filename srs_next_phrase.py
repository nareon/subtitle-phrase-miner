#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path
from collections import defaultdict, Counter
import math


STATE_NEW = 0
STATE_INTRO = 1
STATE_LEARN = 2
STATE_KNOWN = 3
STATE_MATURE = 4


def load_word_index(words_path: Path):
    """Загрузка words.tsv -> (word2id, id2word, freq, rank)."""
    word2id = {}
    id2word = {}
    word_freq = {}
    word_rank = {}

    with words_path.open("r", encoding="utf-8") as f:
        header = f.readline()
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            wid_str, w, f_str, r_str = line.split("\t")
            wid = int(wid_str)
            freq = int(f_str)
            rank = int(r_str)
            word2id[w] = wid
            id2word[wid] = w
            word_freq[wid] = freq
            word_rank[wid] = rank
    return word2id, id2word, word_freq, word_rank


def load_phrase_index(phrases_path: Path):
    """Загрузка phrases.tsv -> dict[pid] = (phrase, freq, cluster_size, length)."""
    phrases = {}
    with phrases_path.open("r", encoding="utf-8") as f:
        header = f.readline()
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            pid_str, phrase, f_str, cs_str, ln_str = line.split("\t")
            pid = int(pid_str)
            freq = int(f_str)
            csize = int(cs_str)
            length = int(ln_str)
            phrases[pid] = (phrase, freq, csize, length)
    return phrases


def load_phrase_words(pw_path: Path):
    """Загрузка phrase_words.tsv -> dict[pid] = [wid1, wid2, ...] и word->phrases."""
    phrase2words = defaultdict(list)
    word2phrases = defaultdict(list)

    with pw_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            pid_str, wid_str = line.split("\t")
            pid = int(pid_str)
            wid = int(wid_str)
            phrase2words[pid].append(wid)
            word2phrases[wid].append(pid)

    return phrase2words, word2phrases


def load_word_set(path: Path, word2id: dict) -> set[int]:
    """Загрузить множество слов (как id) из txt со словами по одному в строке."""
    s: set[int] = set()
    if not path.exists():
        return s
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            w = line.strip()
            if not w:
                continue
            wid = word2id.get(w)
            if wid is not None:
                s.add(wid)
    return s


def difficulty_for_phrase(
    word_ids,
    phrase_freq,
    length,
    word_states,
    a1=3.0,
    a2=2.0,
    a3=1.0,
    b1=0.3,
    c1=0.5,
):
    """
    Оценка сложности фразы.
    word_states: dict[wid] -> STATE_*
    """
    n_new = n_intro = n_learn = 0
    for wid in word_ids:
        st = word_states.get(wid, STATE_NEW)
        if st == STATE_NEW:
            n_new += 1
        elif st == STATE_INTRO:
            n_intro += 1
        elif st == STATE_LEARN:
            n_learn += 1

    # базовый штраф за новые/сыроватые слова
    diff = (
        a1 * n_new
        + a2 * n_intro
        + a3 * n_learn
    )

    # длина (оптимум около 4 слов)
    diff += b1 * (length - 4) ** 2

    # бонус за частотную фразу
    diff -= c1 * math.log(phrase_freq + 1.0)

    return diff, n_new, n_intro, n_learn


def choose_next_phrase(
    word2id,
    id2word,
    word_rank,
    phrases,
    phrase2words,
    word2phrases,
    known_ids: set[int],
    intro_ids: set[int],
    learn_ids: set[int],
    max_new=1,
    max_new_plus_intro=2,
    max_learn=2,
    top_unknown_candidates=200,
):
    """
    Главная функция выбора следующей фразы.
    Двухступенчатый режим:
      1) строгие пороги (1 новое слово и т.д.)
      2) если не найдено — расслабляем пороги и выбираем самую лёгкую фразу.
    """

    # 1. Состояния слов
    word_states = {}
    for wid in known_ids:
        word_states[wid] = STATE_KNOWN
    for wid in learn_ids:
        word_states[wid] = STATE_LEARN
    for wid in intro_ids:
        word_states[wid] = STATE_INTRO

    # 2. Список кандидатов-слов: самые частотные ещё НЕ известные
    all_word_ids = sorted(word_rank.keys(), key=lambda w: word_rank[w])  # по rank
    unknown_candidates = []
    for wid in all_word_ids:
        if wid not in word_states:  # NEW
            unknown_candidates.append(wid)
        if len(unknown_candidates) >= top_unknown_candidates:
            break

    if not unknown_candidates:
        print("[warn] no unknown words left", file=sys.stderr)
        return None

    # ------------------------
    # Проход 1: строгий режим
    # ------------------------
    best_strict = None

    for target_wid in unknown_candidates:
        phrase_ids = word2phrases.get(target_wid, [])
        if not phrase_ids:
            continue

        for pid in phrase_ids:
            phrase, freq, csize, length = phrases[pid]
            wids = phrase2words[pid]

            diff, n_new, n_intro, n_learn = difficulty_for_phrase(
                wids, freq, length, word_states
            )

            # жёсткие ограничения А1–А2
            if n_new > max_new:
                continue
            if n_new + n_intro > max_new_plus_intro:
                continue
            if n_learn > max_learn:
                continue

            if best_strict is None or diff < best_strict["score"]:
                best_strict = {
                    "pid": pid,
                    "phrase": phrase,
                    "target_wid": target_wid,
                    "score": diff,
                    "n_new": n_new,
                    "n_intro": n_intro,
                    "n_learn": n_learn,
                    "freq": freq,
                    "length": length,
                }

    if best_strict is not None:
        return best_strict

    # ------------------------
    # Проход 2: расслабленный режим
    # (например, на старте обучения)
    # ------------------------
    print("[info] no phrase in strict mode, relaxing constraints...", file=sys.stderr)

    best_relaxed = None

    for target_wid in unknown_candidates:
        phrase_ids = word2phrases.get(target_wid, [])
        if not phrase_ids:
            continue

        for pid in phrase_ids:
            phrase, freq, csize, length = phrases[pid]
            wids = phrase2words[pid]

            diff, n_new, n_intro, n_learn = difficulty_for_phrase(
                wids, freq, length, word_states
            )

            # В расслабленном режиме не ограничиваем n_new / n_intro,
            # а полагаемся на функцию сложности:
            #   - короткие и частотные фразы будут иметь меньший diff
            # Можно оставить ограничение на длину, если нужно:
            if length > 5:
                continue

            if best_relaxed is None or diff < best_relaxed["score"]:
                best_relaxed = {
                    "pid": pid,
                    "phrase": phrase,
                    "target_wid": target_wid,
                    "score": diff,
                    "n_new": n_new,
                    "n_intro": n_intro,
                    "n_learn": n_learn,
                    "freq": freq,
                    "length": length,
                }

    return best_relaxed

def main():
    parser = argparse.ArgumentParser(
        description="Выбор следующей фразы по мягкому правилу 1 нового слова."
    )
    parser.add_argument("--index-dir", required=True,
                        help="Каталог с words.tsv, phrases.tsv, phrase_words.tsv.")
    parser.add_argument("--known", default="known_words.txt",
                        help="Файл со списком известных слов (по одному в строке).")
    parser.add_argument("--intro", default="intro_words.txt",
                        help="Файл со списком INTRO-слов.")
    parser.add_argument("--learn", default="learn_words.txt",
                        help="Файл со списком LEARN-слов.")
    parser.add_argument("--max-new", type=int, default=1)
    parser.add_argument("--max-new-plus-intro", type=int, default=2)
    parser.add_argument("--max-learn", type=int, default=2)
    parser.add_argument("--top-unknown", type=int, default=200)
    args = parser.parse_args()

    index_dir = Path(args.index_dir)
    words_path = index_dir / "words.tsv"
    phrases_path = index_dir / "phrases.tsv"
    pw_path = index_dir / "phrase_words.tsv"

    print("[info] loading indices...", file=sys.stderr)
    word2id, id2word, word_freq, word_rank = load_word_index(words_path)
    phrases = load_phrase_index(phrases_path)
    phrase2words, word2phrases = load_phrase_words(pw_path)

    known_ids = load_word_set(Path(args.known), word2id)
    intro_ids = load_word_set(Path(args.intro), word2id)
    learn_ids = load_word_set(Path(args.learn), word2id)

    print(f"[info] KNOWN={len(known_ids)}, INTRO={len(intro_ids)}, LEARN={len(learn_ids)}",
          file=sys.stderr)

    best = choose_next_phrase(
        word2id,
        id2word,
        word_rank,
        phrases,
        phrase2words,
        word2phrases,
        known_ids,
        intro_ids,
        learn_ids,
        max_new=args.max_new,
        max_new_plus_intro=args.max_new_plus_intro,
        max_learn=args.max_learn,
        top_unknown_candidates=args.top_unknown,
    )

    if best is None:
        print("NO_PHRASE_FOUND")
        return

    target_word = id2word[best["target_wid"]]
    print("=== NEXT PHRASE ===")
    print("phrase_id :", best["pid"])
    print("phrase    :", best["phrase"])
    print("target    :", target_word)
    print("score     :", f"{best['score']:.3f}")
    print("freq      :", best["freq"])
    print("length    :", best["length"])
    print("n_new / n_intro / n_learn :",
          best["n_new"], best["n_intro"], best["n_learn"])


if __name__ == "__main__":
    main()
