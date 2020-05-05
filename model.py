"""model.py This module implements a FastText model which can be used
to calculate soft cosine matrix simularity.

"""

from multiprocessing import cpu_count
import os

from gensim.corpora import Dictionary
from gensim.models import WordEmbeddingSimilarityIndex, Word2Vec
from gensim.similarities import SoftCosineSimilarity, SparseTermSimilarityMatrix


class W2vModel:
    """fastText model class for training and soft cosine simularity
    calculation. Mostly copied from this tutorial: https://git.io/JfOPw

    """

    def __init__(self):
        self.dictionary = None
        self.ft = None
        self.matrix = None

    def train(self, sentences):
        """Train a fastText model with sentences"""

        dictionary = Dictionary(sentences)

        ft = Word2Vec(sentences, workers=cpu_count(), min_count=5, size=300, seed=12345)

        index = WordEmbeddingSimilarityIndex(ft.wv)
        matrix = SparseTermSimilarityMatrix(index, dictionary)

        self.dictionary = dictionary
        self.ft = ft
        self.matrix = matrix

    def similarity(self, query, documents):
        """Caclulate cosine simularity between query and all documents"""

        bow_query = self.dictionary.doc2bow(query)
        bow_docs = [self.dictionary.doc2bow(document) for document in documents]

        index = SoftCosineSimilarity(bow_docs, self.matrix)
        similarities = index[bow_query]

        return similarities

    def load(self, directory):
        """Load model files from a directory"""

        self.ft = Word2Vec.load(os.path.join(directory, "w2v.model"))
        self.dictionary = Dictionary.load(os.path.join(directory, "dict.model"))
        self.matrix = SparseTermSimilarityMatrix.load(
            os.path.join(directory, "stsm.model")
        )

    def save(self, directory):
        """Save model files to a directory"""

        os.makedirs(directory, exist_ok=True)

        self.ft.save(os.path.join(directory, "w2v.model"))
        self.matrix.save(os.path.join(directory, "stsm.model"))
        self.dictionary.save(os.path.join(directory, "dict.model"))
