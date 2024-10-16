import numpy as np

class QuantileAnchorElements:
    def __init__(self,q):
        self.name = f"quantile {q}"
        self.q = q
        self.initial_q = q

    def calc_anchor_elements(self, elements):
        nodes = []
        potential_anchors = []
        for element in elements.values():
            if element.is_active():
                nodes.append(element.degree)
                potential_anchors.append(element)
        quantile = np.quantile(nodes, q=self.q)
        return set(potential_anchors[i] for i in range(len(nodes)) if nodes[i] >= quantile)


    def double_quantile(self):
        self.q = 2 * self.q - 1
        if self.q < 0:
            self.q = 0

    def reset_quantile(self):
        self.q = self.initial_q