"""analysis.py: process subreddit jsonl dumps"""

import json
import re

import dask
import dask.bag as db
import dask.dataframe as dd
import pandas as pd


def kws(record, props, pattern):
    """Assign keywords and drop columns"""
    text = (record[prop] for prop in props if record.get(prop))
    matches = re.findall(pattern, "".join(text), flags=re.IGNORECASE)
    keywords = set(kw.lower() for kw in matches)

    if not keywords:
        keywords = ["nokeyword"]

    return {
        "created_utc": record["created_utc"],
        "subreddit": record["subreddit"],
        "keyword": list(keywords),
    }


def process(jsonl, pattern, props):
    """Subreddit data processing pipeline.

    Use bag for initial keyword calculation and filtering, was getting
    type errors when using dd.read_json. See this link for more
    information: https://stackoverflow.com/q/54992783/4501508).

    """

    bag = db.read_text(jsonl, blocksize="10MiB").map(json.loads)
    df = bag.map(kws, props=props, pattern=pattern).to_dataframe()

    df["created_utc"] = dd.to_datetime(df["created_utc"], unit="s")
    df = df.set_index("created_utc")

    df = df.explode("keyword")
    df["keyword"] = df["keyword"].astype("category").cat.as_known()

    agg = (
        df.groupby([pd.Grouper(freq="1d"), "subreddit", "keyword"])
        .size()
        .to_frame("count")
        .reset_index()
    )

    return agg


def main(termf, subsf, comsf, subprops, comprops, subsout, comsout):
    """Run everything, change dask config if multiprocessing is causing problems"""
    with open(termf) as f:
        terms = [term.strip() for term in f.readlines()]
        pattern = "|".join(terms)

    with dask.config.set(scheduler="processes"):
        subs = process(subsf, pattern, subprops)
        subs.to_csv(subsout, index=False, single_file=True)

        coms = process(comsf, pattern, comprops)
        coms.to_csv(comsout, index=False, single_file=True)


if __name__ == """__main__""":
    main(
        termf="config/coronaterms.txt",
        subsf="data/subs.jsonl",
        comsf="data/coms.jsonl",
        subprops=["title", "selftext"],
        comprops=["body"],
        subsout="data/subs_agg.csv",
        comsout="data/coms_agg.csv",
    )
