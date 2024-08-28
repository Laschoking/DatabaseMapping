from src.LexicalSimilarityMetrics.CodeBert import *
import torch
if __name__ == "__main__":
    input1 = torch.randn(100, 1)
    input2 = torch.randn(100, 1)
    cos = torch.nn.CosineSimilarity(dim=0, eps=1e-6)
    output = cos(input1, input2)

    CB = CodeBertSimilarity()
    print(CB.compute_similarity("hallo","test",[]))
