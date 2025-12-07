# subtitle-phrase-miner

python clean_phrases_step1.py \
    -i data/es.txt \
    -o data/subtitles_step1_clean.txt \
    --min-words 2 \
    --max-words 6 \
    --chunk-size 200000 \
    --workers 16 \
    --progress-interval 2000000

python step2_count_phrases.py \
    -i data/subtitles_step1_clean.txt \
    -o data/subtitles_step2_freq.txt \
    --chunk-size 200000 \
    --workers 16 \
    --progress-interval 2000000 \
    --tail-percent 5.0

python filter_min_count.py \
    -i data/subtitles_step2_freq.txt \
    -o data/subtitles_step2_freq_min5.txt \
    --min-count 5 \
    --progress-interval 2000000

# 1) частоты слов
python3 step3_word_freq.py \
  -i data/subtitles_step2_freq_min5.txt \
  -o data/words_freq.txt

# 2) фильтрация фраз по top-5000 слов
python3 step4_filter_phrases_by_vocab.py \
  -i data/subtitles_step2_freq_min5.txt \
  -o data/subtitles_step3_top5000.txt \
  --word-freq data/words_freq.txt \
  --top-n 5000 \
  --min-count 5

python3 step5_count_phrase_lengths.py \
    -i data/subtitles_step3_top5000.txt

=== RESULT ===
Total phrases: 2,734,362

2 words : 218,214
3 words : 604,026
4 words : 810,146
5 words : 667,989
6 words : 433,987

Percentages:
2 words : 7.98 %
3 words : 22.09 %
4 words : 29.63 %
5 words : 24.43 %
6 words : 15.87 %

python3 encode_bge_m3.py \
    -i data/subtitles_step3_top5000.txt \
    -d data/bge_m3_embeddings \
    --batch-size 128

# [done] embeddings saved to data/bge_m3_embeddings/bge_m3_embeddings.dat
# [done] meta saved to      data/bge_m3_embeddings/bge_m3_meta.tsv
# [done] encoded rows:      2,734,362
# [info] total lines read:  2,734,362

python3 cluster_leader_faiss.py \
  --emb data/bge_m3_embeddings/bge_m3_embeddings.dat \
  --dim 1024 \
  --out data/bge_m3_embeddings/cluster_ids.txt \
  --k 32 \
  --threshold 0.92

# [info] total clusters: 1,495,739
# [done] cluster ids written to data/bge_m3_embeddings/cluster_ids.tx


python3 aggregate_clusters.py \
  --meta data/bge_m3_embeddings/bge_m3_meta.tsv \
  --clusters data/bge_m3_embeddings/cluster_ids.txt \
  --out data/bge_m3_embeddings/clusters_aggregated.tsv

# [done] written: data/bge_m3_embeddings/clusters_aggregated.tsv

Максимально мягкий вариант (оставить всё, просто отсортировать):
python3 select_final_phrases.py \
  -i data/bge_m3_embeddings/clusters_aggregated.tsv \
  -o data/final_phrases_all.tsv
# [read] 500,000 lines...
# [read] 1,000,000 lines...
# [info] total clusters read: 1,495,739
# [info] clusters after filters: 1,495,739
# [done] written 1,495,739 phrases to data/final_phrases_all.tsv

Отбросить одиночные кластеры и очень редкие:
python3 select_final_phrases.py \
  -i data/bge_m3_embeddings/clusters_aggregated.tsv \
  -o data/final_phrases_min10_sz2.tsv \
  --min-freq 10 \
  --min-size 2
# [read] 500,000 lines...
# [read] 1,000,000 lines...
# [info] total clusters read: 1,495,739
# [info] clusters after filters: 408,108
# [done] written 408,108 phrases to data/final_phrases_min10_sz2.tsv

Сделать компактный «учебный словарь» на, скажем, 300 000 фраз:

python3 select_final_phrases.py \
  -i data/bge_m3_embeddings/clusters_aggregated.tsv \
  -o data/final_phrases_top300k.tsv \
  --top-k 300000 \
  --min-freq 5 \
  --min-size 1
# [read] 500,000 lines...
# [read] 1,000,000 lines...
# [info] total clusters read: 1,495,739
# [info] clusters after filters: 1,495,739
# [info] taking top-300000 clusters
# [done] written 300,000 phrases to data/final_phrases_top300k.tsv

python3 build_indices_for_srs.py \
  -i data/final_phrases_top300k.tsv \
  --out-dir data/index_srs
# [info] pass 1: counting word frequencies from data/final_phrases_top300k.tsv
# [pass1] 200,000 phrases...
# [info] total phrases read: 300,000
# [info] vocab size: 4,999
# [info] building word index...
# [done] words.tsv written: 4,999 words
# [info] pass 2: writing phrases and phrase_words...
# [pass2] 200,000 phrases...
# [done] phrases.tsv written, total phrases: 300,000
# [done] phrase_words.tsv written

Скрипт выбора следующей фразы srs_next_phrase.py

Сначала можно сделать пустые файлы состояний (ничего ещё не выучено):
touch known_words.txt intro_words.txt learn_words.txt

python3 srs_next_phrase.py \
  --index-dir data/index_srs \
  --known known_words.txt \
  --intro intro_words.txt \
  --learn learn_words.txt
# [info] loading indices...
# [info] KNOWN=0, INTRO=0, LEARN=0
# [info] no phrase in strict mode, relaxing constraints...
# === NEXT PHRASE ===
# phrase_id : 0
# phrase    : está bien
# target    : bien
# score     : 0.396
# freq      : 813176
# length    : 2
# n_new / n_intro / n_learn : 2 0 0

echo "bien" >> intro_words.txt
echo "está" >> intro_words.txt  # если хочешь вводить оба

python3 srs_next_phrase.py --index-dir data/index_srs \
  --known known_words.txt \
  --intro intro_words.txt \
  --learn learn_words.txt
# [info] loading indices...
# [info] KNOWN=0, INTRO=2, LEARN=0
# === NEXT PHRASE ===
# phrase_id : 6
# phrase    : muy bien
# target    : muy
# score     : -0.230
# freq      : 384424
# length    : 2
# n_new / n_intro / n_learn : 1 1 0

wc -m data/final_phrases_top300k.tsv
# 7253712 data/final_phrases_top300k.tsv

python3 restore_question_marks.py \
  -i data/final_phrases_top300k.tsv \
  -o data/final_phrases_top300k_qrestored.tsv
# [progress] 200,000 lines processed, 16,297 marked as questions
# [done] total lines: 300,000
# [done] questions marked: 23,158
# [done] percentage: 7.72 %

load_corpus_to_db.py
# [INFO] Connecting to PostgreSQL...
# [INFO] Creating schema...
# [OK] Connected.
# [OK] Schema ready.
# [INFO] Building data/phrases_for_db.tsv and data/phrase_words_for_db.tsv ...
# [INFO] Built 300000 phrases (from 300000 lines).
# [INFO] Truncating tables...
# [OK] Tables truncated.
# [LOAD] Importing into words from data/index_srs/words.tsv ...
# [LOAD] Importing into phrases from data/phrases_for_db.tsv ...
# [LOAD] Importing into phrase_words from data/phrase_words_for_db.tsv ...
#
# === DATABASE STATISTICS ===
# words               : 4,999
# phrases             : 300,000
# phrase_words        : 1,028,235

python3 delete_repeated_phrases.py
# [INFO] Connecting to PostgreSQL…
# [INFO] Searching for phrases with repeated words…
# [INFO] Found 5,638 phrases with repeated words.
# [INFO] Deleting from phrase_words…
# [OK] phrase_words deleted: 22,121
# [INFO] Deleting from phrases…
# [OK] phrases deleted: 5,184
# [DONE] Completed.
# [DONE] Removed phrases with repeated words: 5,638


INSERT INTO users (name) VALUES ('default_user') RETURNING id;
python3 srs_next_phrase_db.py --user-id 1
# INTRO : 1
# [INFO] Word state stats for user_id=1:
#  NEW   : 4,998
#
# === NEXT PHRASE ===
# phrase_id : 19
# phrase    : así es
# target    : 'así'
# freq      : 153696
# n_new / n_intro / n_learn : 1 / 1 / 0
# mode      : STRICT
#
# [INFO] History updated, target word marked as INTRO (if it was NEW).

