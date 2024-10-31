# See https://github.com/tech-srl/code2vec#choosing-implementation-to-use for details
from scipy.spatial.distance import cosine, euclidean
from gensim.models import KeyedVectors as word2vec

if __name__ == "__main__":
    vectors_text_path = '/home/kotname/Documents/Diplom/Code/DatabaseMapping/Code2Vec_token_embeddings.txt'
    model = word2vec.load_word2vec_format(vectors_text_path, binary=False)
    model.most_similar(positive=['equals', 'tolower']) # or: 'tolower', if using the downloaded embeddings
    model.most_similar(positive=['download', 'send'], negative=['receive'])
    # Get the vectors for the words
    vector1 = model['<com.matt_richardson.gocd.websocket_notifier.GoNotificationPlugin$1: int responseCode()>/$stack1']
    vector2 = model['<com.matt_richardson.gocd.websocket_notifier.GoNotificationPlugin$1: int responseCode()>/this#_0']

    # Calculate cosine similarity
    cosine_similarity = 1 - cosine(vector1, vector2)
    print(cosine_similarity)


    # Throws Value Error if string not inside