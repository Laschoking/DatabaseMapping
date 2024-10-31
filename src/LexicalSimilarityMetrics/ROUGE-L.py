from src.Classes.SimilarityMetric import LexicalSimilarityMetric
from transformers import AutoTokenizer, AutoModel, utils
import torch
import tensorflow as tf
import tensorflow_text as text


class RougeSimilarity(LexicalSimilarityMetric):
    def __init__(self):
        super().__init__("ROUGE-L")

        self.tokenizer = AutoTokenizer.from_pretrained("microsoft/codebert-base")
        self.model = AutoModel.from_pretrained("microsoft/codebert-base")


    def compute_lexical_similarity(self, element_name1, element_name2):

