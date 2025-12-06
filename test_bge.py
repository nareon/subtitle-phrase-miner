from sentence_transformers import SentenceTransformer

model = SentenceTransformer("BAAI/bge-m3")
print(model.get_sentence_embedding_dimension())
