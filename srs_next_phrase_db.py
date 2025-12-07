#!/usr/bin/env python3
import os
import sys
import psycopg2
from dotenv import load_dotenv
import argparse
from datetime import datetime, timezone


# =============================
# 1. Подключение к БД (.env)
# =============================
load_dotenv()

PG_DB       = os.getenv("PG_DB")
PG_USER     = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = os.getenv("PG_PORT", "5432")

if not PG_DB or not PG_USER or not PG_PASSWORD:
    print("[ERROR] Missing DB params in .env", file=sys.stderr)
    sys.exit(1)

DSN = (
    f"dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} "
    f"host={PG_HOST} port={PG_PORT}"
)


# =============================
# 2. SQL
# =============================

# Статистика состояний слов по пользователю
SQL_WORD_STATE_STATS = """
    SELECT COALESCE(state::text, 'NEW') AS st, COUNT(*) AS cnt
    FROM (
        -- все слова с их состоянием (или NEW, если записи нет)
        SELECT w.id,
               uws.state
        FROM words w
        LEFT JOIN user_word_state uws
          ON uws.word_id = w.id AND uws.user_id = %(user_id)s
    ) t
    GROUP BY COALESCE(state::text, 'NEW')
    ORDER BY st;
"""

# Кандидатная фраза: ровно 1 NEW, фраза ещё не показывалась пользователю
SQL_FIND_CANDIDATE_STRICT = """
WITH pw AS (
    SELECT
        pw.phrase_id,
        pw.word_id,
        COALESCE(uws.state::text, 'NEW') AS state
    FROM phrase_words pw
    LEFT JOIN user_word_state uws
      ON uws.word_id = pw.word_id
     AND uws.user_id = %(user_id)s
),
agg AS (
    SELECT
        pw.phrase_id,
        SUM(CASE WHEN state = 'NEW'   THEN 1 ELSE 0 END) AS n_new,
        SUM(CASE WHEN state = 'INTRO' THEN 1 ELSE 0 END) AS n_intro,
        SUM(CASE WHEN state = 'LEARN' THEN 1 ELSE 0 END) AS n_learn
    FROM pw
    GROUP BY pw.phrase_id
),
candidates AS (
    SELECT
        p.id,
        p.phrase,
        p.freq,
        a.n_new,
        a.n_intro,
        a.n_learn
    FROM agg a
    JOIN phrases p ON p.id = a.phrase_id
    LEFT JOIN user_phrase_history h
      ON h.user_id = %(user_id)s
     AND h.phrase_id = p.id
    WHERE a.n_new = 1
      AND h.phrase_id IS NULL   -- фраза ещё ни разу не показывалась
)
SELECT id, phrase, freq, n_new, n_intro, n_learn
FROM candidates
ORDER BY freq DESC
LIMIT 1;
"""

# Ослабленный вариант: допускаем повторно показывать фразы (если строгий не нашёл)
SQL_FIND_CANDIDATE_RELAXED = """
WITH pw AS (
    SELECT
        pw.phrase_id,
        pw.word_id,
        COALESCE(uws.state::text, 'NEW') AS state
    FROM phrase_words pw
    LEFT JOIN user_word_state uws
      ON uws.word_id = pw.word_id
     AND uws.user_id = %(user_id)s
),
agg AS (
    SELECT
        pw.phrase_id,
        SUM(CASE WHEN state = 'NEW'   THEN 1 ELSE 0 END) AS n_new,
        SUM(CASE WHEN state = 'INTRO' THEN 1 ELSE 0 END) AS n_intro,
        SUM(CASE WHEN state = 'LEARN' THEN 1 ELSE 0 END) AS n_learn
    FROM pw
    GROUP BY pw.phrase_id
)
SELECT
    p.id,
    p.phrase,
    p.freq,
    a.n_new,
    a.n_intro,
    a.n_learn
FROM agg a
JOIN phrases p ON p.id = a.phrase_id
WHERE a.n_new = 1
ORDER BY p.freq DESC
LIMIT 1;
"""

# Поиск целевого нового слова в выбранной фразе
SQL_FIND_TARGET_WORD = """
SELECT w.id, w.word
FROM phrase_words pw
JOIN words w ON w.id = pw.word_id
LEFT JOIN user_word_state uws
  ON uws.word_id = pw.word_id
 AND uws.user_id = %(user_id)s
WHERE pw.phrase_id = %(phrase_id)s
  AND COALESCE(uws.state::text, 'NEW') = 'NEW'
LIMIT 1;
"""

# Запись факта показа фразы (можно вызывать сразу после выбора)
SQL_INSERT_HISTORY = """
INSERT INTO user_phrase_history (user_id, phrase_id, shown_at, result)
VALUES (%(user_id)s, %(phrase_id)s, %(shown_at)s, %(result)s);
"""

# При первом показе целевого слова переводим его в INTRO
SQL_UPSERT_WORD_STATE_INTRO = """
INSERT INTO user_word_state (user_id, word_id, state, reps, lapses, last_result, last_seen)
VALUES (%(user_id)s, %(word_id)s, 'INTRO', 0, 0, NULL, %(seen_at)s)
ON CONFLICT (user_id, word_id) DO NOTHING;
"""


# =============================
# 3. Логика
# =============================

def main():
    parser = argparse.ArgumentParser(
        description="Выбор следующей фразы из БД по правилу SRS (1 новое слово)."
    )
    parser.add_argument("--user-id", type=int, default=1, help="ID пользователя (по умолчанию 1)")
    parser.add_argument("--no-history", action="store_true",
                        help="Не записывать показ фразы в user_phrase_history и не трогать word_state.")
    args = parser.parse_args()

    user_id = args.user_id

    try:
        conn = psycopg2.connect(DSN)
    except Exception as e:
        print("[ERROR] DB connect failed:", e, file=sys.stderr)
        sys.exit(1)

    conn.autocommit = False
    cur = conn.cursor()

    # Статистика по состояниям слов
    print(f"[INFO] Word state stats for user_id={user_id}:")
    cur.execute(SQL_WORD_STATE_STATS, {"user_id": user_id})
    rows = cur.fetchall()
    for st, cnt in rows:
        print(f"  {st:6s}: {cnt:,}")
    print()

    # Пытаемся найти фразу в строгом режиме
    cur.execute(SQL_FIND_CANDIDATE_STRICT, {"user_id": user_id})
    row = cur.fetchone()

    relaxed = False
    if row is None:
        print("[INFO] No phrase in strict mode, trying relaxed (allow already seen phrases)...")
        cur.execute(SQL_FIND_CANDIDATE_RELAXED, {"user_id": user_id})
        row = cur.fetchone()
        relaxed = True

    if row is None:
        print("NO_PHRASE_FOUND")
        conn.close()
        return

    phrase_id, phrase, freq, n_new, n_intro, n_learn = row

    # Находим целевое новое слово
    cur.execute(SQL_FIND_TARGET_WORD, {"user_id": user_id, "phrase_id": phrase_id})
    wrow = cur.fetchone()
    if wrow is None:
        target_word_id = None
        target_word = None
    else:
        target_word_id, target_word = wrow

    print("=== NEXT PHRASE ===")
    print(f"phrase_id : {phrase_id}")
    print(f"phrase    : {phrase}")
    print(f"target    : {target_word!r}")
    print(f"freq      : {freq}")
    print(f"n_new / n_intro / n_learn : {n_new} / {n_intro} / {n_learn}")
    print(f"mode      : {'RELAXED' if relaxed else 'STRICT'}")

    # По желанию сразу записываем показ в историю и переводим target_word в INTRO
    if not args.no_history and target_word_id is not None:
        now = datetime.now(timezone.utc)
        cur.execute(
            SQL_INSERT_HISTORY,
            {
                "user_id": user_id,
                "phrase_id": phrase_id,
                "shown_at": now,
                "result": "shown",  # сюда позже подставим 'good' / 'hard' / 'again'
            },
        )
        cur.execute(
            SQL_UPSERT_WORD_STATE_INTRO,
            {
                "user_id": user_id,
                "word_id": target_word_id,
                "seen_at": now,
            },
        )
        conn.commit()
        print("\n[INFO] History updated, target word marked as INTRO (if it was NEW).")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
