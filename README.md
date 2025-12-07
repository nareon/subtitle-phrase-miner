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
  --min-size
# [read] 500,000 lines...
# [read] 1,000,000 lines...
# [info] total clusters read: 1,495,739
# [info] clusters after filters: 1,495,739
# [info] taking top-300000 clusters
# [done] written 300,000 phrases to data/final_phrases_top300k.tsv

