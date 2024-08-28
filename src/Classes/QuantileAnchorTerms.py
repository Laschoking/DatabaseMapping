import numpy as np

class QuantileAnchorTerms:
    def __init__(self,q):
        self.name = f"quantile {q}"
        self.q = q

    def calc_anchor_terms(self, terms):
        nodes = []
        potential_anchors = []
        for term in terms.values():
            if term.is_active():
                nodes.append(term.degree)
                potential_anchors.append(term)
        quantile = np.quantile(nodes, q=self.q)
        return set(potential_anchors[i] for i in range(len(nodes)) if nodes[i] >= quantile)

