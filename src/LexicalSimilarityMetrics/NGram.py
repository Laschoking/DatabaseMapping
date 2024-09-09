import difflib
from src.Classes.SimilarityMetric import LexicalSimilarityMetric
from strsimpy.longest_common_subsequence import LongestCommonSubsequence
import re
import numpy as np

class NGram(LexicalSimilarityMetric):
    def __init__(self,N=2):
        super().__init__("N-Gram Similarity")
        self.N = N

    def compute_lexical_similarity(self, term_name1, term_name2):
        # Generate N-grams for both sequences
        X = self.affix_string(term_name1)
        Y = self.affix_string(term_name2)

        K = len(X)
        L = len(Y)

        # Initialize the similarity matrix S
        S = np.zeros(shape=(K + 1,L + 1),dtype=float)

        for i in range(1,K + 1):
            for j in range(1,L + 1):
                ngram_score = self.pos_ngram_score(i-1,j-1,term_name1,term_name2)
                S[i,j] = max(S[i-1,j],S[i,j-1],S[i-1,j-1] + ngram_score)

        # Return the similarity score as the normalized value
        return S[K][L] / max(K, L)

    def pos_ngram_score(self,x,y,term_name1,term_name2):
        return 1

    def affix_string(self,sequence):
        ascii_to_special = {
            ' ': '☺', '!': '☻', '"': '♥', '#': '♦', '$': '♣', '%': '♠', '&': '•', "'": '◘',
            '(': '○', ')': '◙', '*': '♂', '+': '♀', ',': '♪', '-': '♫', '.': '☼', '/': '►',
            '0': '◄', '1': '↕', '2': '‼', '3': '¶', '4': '§', '5': '▬', '6': '↨', '7': '↑',
            '8': '↓', '9': '→', ':': '←', ';': '∟', '<': '↔', '=': '▲', '>': '▼', '?': '⌂',
            '@': '■', 'A': '□', 'B': '▢', 'C': '▣', 'D': '▪', 'E': '▫', 'F': '◊', 'G': '⊙',
            'H': '⊛', 'I': '⊠', 'J': '⊡', 'K': '⊞', 'L': '⊟', 'M': '⊠', 'N': '⊡', 'O': '⊣',
            'P': '⊤', 'Q': '⊥', 'R': '⊦', 'S': '⊧', 'T': '⊩', 'U': '⊪', 'V': '⊫', 'W': '⊬',
            'X': '⊭', 'Y': '⊮', 'Z': '⊯', '[': '⊰', '\\': '⊱', ']': '⊲', '^': '⊳', '_': '⊴',
            '`': '⊵', 'a': '⊶', 'b': '⊷', 'c': '⊸', 'd': '⊹', 'e': '⊺', 'f': '⊻', 'g': '⊼',
            'h': '⊽', 'i': '⊾', 'j': '⊿', 'k': '⋀', 'l': '⋁', 'm': '⋂', 'n': '⋃', 'o': '⋄',
            'p': '⋅', 'q': '⋆', 'r': '⋇', 's': '⋈', 't': '⋉', 'u': '⋊', 'v': '⋋', 'w': '⋌',
            'x': '⋍', 'y': '⋎', 'z': '⋏', '{': '⋐', '|': '⋑', '}': '⋒', '~': '⋓'
        }

        if sequence[0] not in ascii_to_special:
            raise KeyError(f"character {sequence[0]} not found in special dict")

        sequence = ascii_to_special[sequence[0]] * (self.N - 1) + sequence

        return sequence


