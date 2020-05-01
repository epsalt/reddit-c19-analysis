"""model.py This module implements a FastText model which can be used
to calculate soft cosine matrix simularity.

"""

from multiprocessing import cpu_count
import os

from gensim.corpora import Dictionary
from gensim.models import FastText, TfidfModel, WordEmbeddingSimilarityIndex
from gensim.similarities import SoftCosineSimilarity, SparseTermSimilarityMatrix


class FastTextModel:
    """fastText model class for training and soft cosine simularity
    calculation. Mostly copied from this tutorial: https://git.io/JfOPw

    """

    def __init__(self):
        self.dictionary = None
        self.ft = None
        self.matrix = None
        self.tfidf = None

    def train(self, sentences):
        """Train a TF-IDF and fastText model with sentences"""

        dictionary = Dictionary(sentences)
        tfidf = TfidfModel([dictionary.doc2bow(text) for text in sentences])

        ft = FastText(size=160, min_count=3)
        ft.build_vocab(sentences=sentences)
        ft.train(
            sentences=sentences,
            epochs=5,
            total_examples=ft.corpus_count,
            total_words=ft.corpus_total_words,
            workers=cpu_count(),
        )

        index = WordEmbeddingSimilarityIndex(ft.wv)
        matrix = SparseTermSimilarityMatrix(index, dictionary, tfidf=tfidf)

        self.dictionary = dictionary
        self.ft = ft
        self.matrix = matrix
        self.tfidf = tfidf

    def similarity(self, query, documents):
        """Caclulate cosine simularity between query and all documents"""

        query = self.tfidf[self.dictionary.doc2bow(query)]
        index = SoftCosineSimilarity(
            self.tfidf[[self.dictionary.doc2bow(document) for document in documents]],
            self.matrix,
        )
        similarities = index[query]

        return similarities

    def load(self, directory):
        """Load model files from a directory"""

        self.ft = FastText.load(os.path.join(directory, "ft.model"))
        self.tfidf = TfidfModel.load(os.path.join(directory, "tfidf.model"))
        self.dictionary = Dictionary.load(os.path.join(directory, "dict.model"))
        self.matrix = SparseTermSimilarityMatrix.load(
            os.path.join(directory, "stsm.model")
        )

    def save(self, directory):
        """Save model files to a directory"""

        self.ft.save(os.path.join(directory, "ft.model"))
        self.tfidf.save(os.path.join(directory, "tfidf.model"))
        self.matrix.save(os.path.join(directory, "stsm.model"))
        self.dictionary.save(os.path.join(directory, "dict.model"))
