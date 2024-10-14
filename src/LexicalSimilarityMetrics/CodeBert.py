from src.Classes.SimilarityMetric import LexicalSimilarityMetric
from transformers import AutoTokenizer, AutoModel, utils
import torch


class CodeBertSimilarity(LexicalSimilarityMetric):
    def __init__(self):
        super().__init__("CodeBert")

        self.tokenizer = AutoTokenizer.from_pretrained("microsoft/codebert-base",  clean_up_tokenization_spaces=True)
        self.model = AutoModel.from_pretrained("microsoft/codebert-base")
        clean_up_tokenization_spaces = True


    def compute_lexical_similarity(self, element_name1, element_name2):
        # Tokenize the input elements properly
        tokens = self.tokenizer([element_name1, element_name2], return_tensors='pt', padding=True, truncation=True)

        # Get the embeddings from the model
        with torch.no_grad():  # Disable gradient computation
            outputs = self.model(**tokens)

        # Extract embeddings for the first token of each element (or the CLS token)
        # Assuming outputs[0] is the last hidden state
        embedding1 = outputs.last_hidden_state[0, 0, :]  # First element's [CLS] token embedding
        embedding2 = outputs.last_hidden_state[1, 0, :]  # Second element's [CLS] token embedding

        # Calculate cosine similarity
        cos = torch.nn.CosineSimilarity(dim=0)
        similarity_score = cos(embedding1, embedding2)

        return similarity_score
