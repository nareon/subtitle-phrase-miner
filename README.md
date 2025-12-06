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
    
python3 cluster_leader_faiss.py \
  --emb data/bge_m3_embeddings/bge_m3_embeddings.dat \
  --dim 1024 \
  --out data/bge_m3_embeddings/cluster_ids.txt \
  --k 32 \
  --threshold 0.92



