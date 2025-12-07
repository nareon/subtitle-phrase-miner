#!/usr/bin/env python3
import os
import sys
import psycopg2
from dotenv import load_dotenv


# =============================
# 1. load .env for DB settings
# =============================
load_dotenv()

PG_DB       = os.getenv("PG_DB")
PG_USER     = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = os.getenv("PG_PORT", "5432")

if not PG_DB or not PG_USER or not PG_PASSWORD:
    print("[ERROR] Missing required DB connection parameters in .env", file=sys.stderr)
    sys.exit(1)

DSN = (
    f"dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} "
    f"host={PG_HOST} port={PG_PORT}"
)


# =============================
# 2. SQL statements
# =============================

SQL_FIND_REPEATED = """
    SELECT phrase_id
    FROM phrase_words
    GROUP BY phrase_id, word_id
    HAVING COUNT(*) > 1;
"""

SQL_DELETE_PHRASE_WORDS = """
    DELETE FROM phrase_words
    WHERE phrase_id = ANY(%s);
"""

SQL_DELETE_PHRASES = """
    DELETE FROM phrases
    WHERE id = ANY(%s);
"""

# Если нужно удалить историю пользователя — раскомментировать
SQL_DELETE_USER_HISTORY = """
    DELETE FROM user_phrase_history
    WHERE phrase_id = ANY(%s);
"""


# =============================
# 3. Main workflow
# =============================

def main():
    print("[INFO] Connecting to PostgreSQL…")

    try:
        conn = psycopg2.connect(DSN)
    except Exception as e:
        print("[ERROR] Cannot connect:", e, file=sys.stderr)
        sys.exit(1)

    conn.autocommit = False
    cur = conn.cursor()

    print("[INFO] Searching for phrases with repeated words…")

    cur.execute(SQL_FIND_REPEATED)
    ids = [row[0] for row in cur.fetchall()]

    total = len(ids)
    print(f"[INFO] Found {total:,} phrases with repeated words.")

    if total == 0:
        print("[DONE] Nothing to delete.")
        conn.close()
        return

    # Convert to array for ANY(%s)
    ids_array = ids

    print("[INFO] Deleting from phrase_words…")
    cur.execute(SQL_DELETE_PHRASE_WORDS, (ids_array,))
    print(f"[OK] phrase_words deleted: {cur.rowcount:,}")

    print("[INFO] Deleting from phrases…")
    cur.execute(SQL_DELETE_PHRASES, (ids_array,))
    print(f"[OK] phrases deleted: {cur.rowcount:,}")

    # Если хотите, можно удалить историю пользователя:
    # print("[INFO] Deleting from user_phrase_history…")
    # cur.execute(SQL_DELETE_USER_HISTORY, (ids_array,))
    # print(f"[OK] user_phrase_history deleted: {cur.rowcount:,}")

    conn.commit()
    conn.close()

    print("[DONE] Completed.")
    print(f"[DONE] Removed phrases with repeated words: {total:,}")


if __name__ == "__main__":
    main()
