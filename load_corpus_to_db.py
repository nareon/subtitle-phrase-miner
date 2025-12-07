#!/usr/bin/env python3
import os
import psycopg2
import sys
from pathlib import Path
from dotenv import load_dotenv


# =============================
# 1. Load .env
# =============================
load_dotenv()

PG_DB       = os.getenv("PG_DB")
PG_USER     = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = os.getenv("PG_PORT", "5432")

if not PG_DB or not PG_USER or not PG_PASSWORD:
    print("[ERROR] Missing required DB parameters in .env")
    sys.exit(1)

DB_DSN = (
    f"dbname={PG_DB} "
    f"user={PG_USER} "
    f"password={PG_PASSWORD} "
    f"host={PG_HOST} "
    f"port={PG_PORT}"
)

# =============================
# 2. File paths
# =============================

WORDS_TSV         = Path("data/index_srs/words.tsv")
FINAL_PHRASES_TSV = Path("data/final_phrases_top300k_qrestored.tsv")

PHRASES_TSV       = Path("data/phrases_for_db.tsv")
PHRASE_WORDS_TSV  = Path("data/phrase_words_for_db.tsv")


# =============================
# 3. SQL schema
# =============================

SQL_CREATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS words (
    id          INTEGER PRIMARY KEY,
    word        TEXT NOT NULL UNIQUE,
    total_freq  INTEGER NOT NULL,
    rank        INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS phrases (
    id           INTEGER PRIMARY KEY,
    phrase       TEXT NOT NULL,
    freq         INTEGER NOT NULL,
    cluster_size INTEGER NOT NULL,
    length       SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS phrase_words (
    phrase_id  INTEGER NOT NULL REFERENCES phrases(id),
    word_id    INTEGER NOT NULL REFERENCES words(id),
    position   SMALLINT NOT NULL DEFAULT 0,
    PRIMARY KEY (phrase_id, word_id, position)
);

CREATE INDEX IF NOT EXISTS idx_phrase_words_word   ON phrase_words (word_id);
CREATE INDEX IF NOT EXISTS idx_phrase_words_phrase ON phrase_words (phrase_id);

CREATE TABLE IF NOT EXISTS users (
    id      SERIAL PRIMARY KEY,
    name    TEXT
);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'word_state_enum') THEN
        CREATE TYPE word_state_enum AS ENUM ('NEW', 'INTRO', 'LEARN', 'KNOWN', 'MATURE');
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS user_word_state (
    user_id     INTEGER NOT NULL REFERENCES users(id),
    word_id     INTEGER NOT NULL REFERENCES words(id),
    state       word_state_enum NOT NULL,
    reps        INTEGER NOT NULL DEFAULT 0,
    lapses      INTEGER NOT NULL DEFAULT 0,
    last_result TEXT,
    last_seen   TIMESTAMPTZ,
    next_due    TIMESTAMPTZ,
    PRIMARY KEY (user_id, word_id)
);

CREATE INDEX IF NOT EXISTS idx_user_word_state_state
    ON user_word_state (user_id, state);

CREATE TABLE IF NOT EXISTS user_phrase_history (
    user_id    INTEGER NOT NULL REFERENCES users(id),
    phrase_id  INTEGER NOT NULL REFERENCES phrases(id),
    shown_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    result     TEXT NOT NULL,
    PRIMARY KEY (user_id, phrase_id, shown_at)
);

CREATE INDEX IF NOT EXISTS idx_user_phrase_history_user_phrase
    ON user_phrase_history (user_id, phrase_id);
"""


# =============================
# 4. Build phrases_for_db.tsv + phrase_words_for_db.tsv
# =============================

def build_phrases_files():
    if not FINAL_PHRASES_TSV.exists():
        print(f"[ERROR] Final phrases file not found: {FINAL_PHRASES_TSV}")
        sys.exit(1)

    if not WORDS_TSV.exists():
        print(f"[ERROR] Words TSV not found: {WORDS_TSV}")
        sys.exit(1)

    print(f"[INFO] Building {PHRASES_TSV} and {PHRASE_WORDS_TSV} ...")

    # load word -> id mapping
    word2id = {}
    with WORDS_TSV.open("r", encoding="utf-8") as f:
        header = f.readline()
        for line in f:
            wid_str, word, freq_str, rank_str = line.rstrip("\n").split("\t")
            word2id[word] = int(wid_str)

    pid = 0
    total = 0

    with FINAL_PHRASES_TSV.open("r", encoding="utf-8") as fin, \
         PHRASES_TSV.open("w", encoding="utf-8") as fp, \
         PHRASE_WORDS_TSV.open("w", encoding="utf-8") as fpw:

        # header for phrases.tsv
        fp.write("id\tphrase\tfreq\tcluster_size\tlength\n")

        for line in fin:
            total += 1
            line = line.rstrip("\n")
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) < 3:
                continue

            phrase       = parts[0]
            freq         = int(parts[1])
            cluster_size = int(parts[2])

            words  = phrase.split()
            length = len(words)

            fp.write(f"{pid}\t{phrase}\t{freq}\t{cluster_size}\t{length}\n")

            # записываем каждое вхождение слова с позицией
            for pos, w in enumerate(words):
                wid = word2id.get(w)
                if wid is not None:
                    fpw.write(f"{pid}\t{wid}\t{pos}\n")

            pid += 1

    print(f"[INFO] Built {pid} phrases (from {total} lines).")


# =============================
# 5. COPY helper
# =============================

def copy_tsv(cur, table, file_path, columns, header=True):
    if not file_path.exists():
        print(f"[ERROR] file not found: {file_path}")
        sys.exit(1)

    print(f"[LOAD] Importing into {table} from {file_path} ...")

    with file_path.open("r", encoding="utf-8") as f:
        cur.copy_expert(
            f"""
            COPY {table} ({columns})
            FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', HEADER {'true' if header else 'false'});
            """,
            f
        )


# =============================
# 6. Main
# =============================

def main():
    print("[INFO] Connecting to PostgreSQL...")
    try:
        conn = psycopg2.connect(DB_DSN)
    except Exception as e:
        print("[ERROR] Could not connect:", e)
        sys.exit(1)

    print("[OK] Connected.")
    cur = conn.cursor()

    print("[INFO] Creating schema...")
    cur.execute(SQL_CREATE_SCHEMA)
    conn.commit()
    print("[OK] Schema ready.")

    # build intermediate files
    build_phrases_files()

    # truncate all dependent tables safely
    print("[INFO] Truncating tables...")
    cur.execute("""
        TRUNCATE TABLE
            user_phrase_history,
            user_word_state,
            phrase_words,
            phrases,
            words
        RESTART IDENTITY;
    """)
    conn.commit()
    print("[OK] Tables truncated.")

    # load words
    copy_tsv(
        cur,
        table="words",
        file_path=WORDS_TSV,
        columns="id, word, total_freq, rank",
        header=True,
    )

    # load phrases
    copy_tsv(
        cur,
        table="phrases",
        file_path=PHRASES_TSV,
        columns="id, phrase, freq, cluster_size, length",
        header=True,
    )

    # load phrase_words (3 колонки: phrase_id, word_id, position)
    copy_tsv(
        cur,
        table="phrase_words",
        file_path=PHRASE_WORDS_TSV,
        columns="phrase_id, word_id, position",
        header=False,
    )

    conn.commit()

    print("\n=== DATABASE STATISTICS ===")
    for tbl in ("words", "phrases", "phrase_words"):
        cur.execute(f"SELECT COUNT(*) FROM {tbl};")
        print(f"{tbl:20s}: {cur.fetchone()[0]:,}")

    print("\n[DONE] Import complete.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
