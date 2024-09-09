from src.Classes.SimilarityMetric import LexicalSimilarityMetric
from transformers import AutoTokenizer, AutoModel, utils
import torch


class CodeBertSimilarity(LexicalSimilarityMetric):
    def __init__(self):
        super().__init__("CodeBert")

        self.tokenizer = AutoTokenizer.from_pretrained("microsoft/codebert-base",  clean_up_tokenization_spaces=True)
        self.model = AutoModel.from_pretrained("microsoft/codebert-base")
        clean_up_tokenization_spaces = True


    def compute_lexical_similarity(self, term_name1, term_name2):
        # Tokenize the input terms properly
        tokens = self.tokenizer([term_name1, term_name2], return_tensors='pt', padding=True, truncation=True)

        # Get the embeddings from the model
        with torch.no_grad():  # Disable gradient computation
            outputs = self.model(**tokens)

        # Extract embeddings for the first token of each term (or the CLS token)
        # Assuming outputs[0] is the last hidden state
        embedding1 = outputs.last_hidden_state[0, 0, :]  # First term's [CLS] token embedding
        embedding2 = outputs.last_hidden_state[1, 0, :]  # Second term's [CLS] token embedding

        # Calculate cosine similarity
        cos = torch.nn.CosineSimilarity(dim=0)
        similarity_score = cos(embedding1, embedding2)

        return similarity_score
'''
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
'''