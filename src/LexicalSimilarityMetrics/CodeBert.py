from src.Classes.SimilarityMetric import LexicalSimilarityMetric
from transformers import AutoTokenizer, AutoModel, utils
import torch


class CodeBertSimilarity(LexicalSimilarityMetric):
    def __init__(self):
        super().__init__("JaroWinkler")

        self.tokenizer = AutoTokenizer.from_pretrained("microsoft/codebert-base")
        self.model = AutoModel.from_pretrained("microsoft/codebert-base")


    def compute_lexical_similarity(self, term_name1, term_name2):

        #embeddings = self.tokenizer.convert_token_to_ids([term_name1, term_name2])


        #embedding2 = self.model(torch.tensor(term_name1)[None, :])[0]
        tokens_ids = self.tokenizer.convert_tokens_to_ids([term_name1,term_name2])
        context_embeddings = self.model(torch.tensor(tokens_ids)[None, :])[0]
        # Calculate cosine similarity
        cos = torch.nn.CosineSimilarity(dim=0)
        embedding1 = context_embeddings[0][0]
        embedding2 = context_embeddings[0][1]
        similarity_score = cos(embedding1, embedding2)
        return similarity_score
