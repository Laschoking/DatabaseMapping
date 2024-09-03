from src.Classes.SimilarityMetric import LexicalSimilarityMetric
from scipy import spatial
import gensim.downloader as api
import numpy as np


model = api.load("glove-wiki-gigaword-50") #choose from multiple models https://github.com/RaRe-Technologies/gensim-data

s0 = 'Mark zuckerberg owns the facebook company'
s1 = 'Facebook company ceo is mark zuckerberg'
s2 = 'Microsoft is owned by Bill gates'
s3 = 'How to learn japanese'

def preprocess(s):
    return [i.lower() for i in s.split()]

def get_vector(s):
    return np.sum(np.array([model[i] for i in preprocess(s)]), axis=0)



class Word2Vec(LexicalSimilarityMetric):
    def __init__(self):
        super().__init__("Word2Vec")


    def compute_lexical_similarity(self, term_name1, term_name2):
        print('s0 vs s1 ->', 1 - spatial.distance.cosine(get_vector(s0), get_vector(s1)))
        print('s0 vs s2 ->', 1 - spatial.distance.cosine(get_vector(s0), get_vector(s2)))
        print('s0 vs s3 ->', 1 - spatial.distance.cosine(get_vector(s0), get_vector(s3)))



if __name__ == '__main__':
    v = Word2Vec()
    v.compute_lexical_similarity("a","b")