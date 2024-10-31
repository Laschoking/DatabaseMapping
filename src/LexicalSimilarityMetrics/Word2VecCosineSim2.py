import gensim
from gensim.models import Word2Vec
from scipy.spatial.distance import cosine

# Load a pre-trained Word2Vec model (e.g., Google News)
# model = gensim.models.KeyedVectors.load_word2vec_format('GoogleNews-vectors-negative300.bin', binary=True)
# For demonstration, we'll create a small Word2Vec model
sentences = [['word1', 'word2', 'word3'], ['word4', 'word5']]
model = Word2Vec(sentences, vector_size=100, window=5, min_count=1, workers=4)

# Obtain word embeddings
vec1 = model.wv['word1']
vec2 = model.wv['word2']

# Compute cosine similarity
cosine_similarity = 1 - cosine(vec1, vec2)

# Compute cosine distance
cosine_distance = 1 - cosine_similarity

print(f"Cosine Similarity: {cosine_similarity}")
print(f"Cosine Distance: {cosine_distance}")
