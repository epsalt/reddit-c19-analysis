"""analysis.py: This module uses a fastText model to predict cosine
similarity between reddit comments and a set of queries

"""

import logging
import json

import dask
import dask.bag as db
import dask.dataframe as dd
import numpy as np
import pandas as pd
import regex
import spacy

from model import W2vModel


def regex_replace(string):
    """Initial comment preprocessing with regex substitutions"""
    patterns = [
        (r"^&gt;.*", ""),  # Quoted comment replys
        (r"\[([^\]]+)\]\(([^)]+)\)", "\\1"),  # Markdown links
        (r"\s+", " "),  # Repeated whitespace
        (r"[^\x00-\x7f]", ""),  # Non-ASCII characters
        (r"(http|www)\S+", ""),  # URLs
    ]

    for pattern, repl in patterns:
        string = regex.sub(pattern, repl, string, flags=regex.MULTILINE)

    return string.strip()


def tokenize(docs):
    """Tokenize documents with spacy"""
    nlp = spacy.load("en_core_web_sm")
    nlp.disable_pipes(["tagger", "parser", "ner"])

    def lemmify(doc):
        return [
            token.lemma_
            for token in doc
            if not (token.is_stop | token.is_punct | token.is_space | token.is_digit)
        ]

    nlp.add_pipe(lemmify)
    nlp.add_pipe(lambda doc: " ".join(doc).lower(), name="stringify")

    return list(nlp.pipe(docs))


def preprocess(jsonl):
    """Ingest data and preprocess comment text"""
    bag = (
        db.read_text(jsonl, blocksize="10MiB")
        .map(json.loads)
        .map(
            lambda r: {
                "created_utc": r["created_utc"],
                "subreddit": r["subreddit"],
                "text": regex_replace(r["body"]),
            }
        )
    )
    df = bag.to_dataframe()

    df = df[df["text"].str.len() > 30]
    df["created_utc"] = dd.to_datetime(df["created_utc"], unit="s")

    ## dask and spacy multiprocessing don't play nicely
    ## nlp.pipe might not be the fastest way to preprocessing
    df = df.compute()
    df["tokens"] = tokenize(df["text"].astype("unicode"))

    df = df[df["tokens"] != ""]
    df = df.drop("text", axis=1)

    return df


def aggregate(df, threshold=0.2):
    """Threshold similarity scores and aggregate similarity df"""
    melted = (
        df.drop("tokens", axis=1)
        .melt(id_vars=["created_utc", "subreddit"], var_name="cat", value_name="sim")
        .assign(cutoff=lambda x: x["sim"] > threshold)
        .set_index("created_utc")
    )

    agg = (
        melted.groupby([pd.Grouper(freq="1d"), "subreddit", "cat"])
        .agg({"cutoff": ["count", np.count_nonzero]})
        .droplevel(0, axis=1)
        .assign(score=lambda x: x["count_nonzero"] / x["count"])
    )

    return agg


def main(termf, comsf, resultsf, train=True, saved_model=None):
    """Run analysis pipeline"""

    with dask.config.set(scheduler="processes"):
        df = preprocess(comsf)

    sentences = [str(doc).split() for doc in df["tokens"].to_list()]

    if train:
        ## Setup logging for model train
        template = "%(asctime)s : %(levelname)s : %(message)s"
        logging.basicConfig(format=template, level=logging.INFO)

        model = W2vModel()
        model.train(sentences)
        model.save("models")

    elif saved_model:
        model = W2vModel()
        model.load(saved_model)

    with open(termf) as f:
        terms = json.load(f)
        queries = terms.keys()
        tokens = [doc.split() for doc in tokenize(list(terms.values()))]

    for query, token in zip(queries, tokens):
        df[query] = model.similarity(token, sentences)

    agg = aggregate(df)
    agg.to_csv(resultsf)


if __name__ == """__main__""":
    main(
        termf="data/terms.json",
        comsf="data/coms.jsonl",
        train=True,
        saved_model=None,
        resultsf="data/scores.csv",
    )
