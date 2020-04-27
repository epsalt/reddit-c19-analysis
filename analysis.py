"""analysis.py: process subreddit jsonl dumps"""

import json
import re

import dask
import dask.bag as db
import dask.dataframe as dd
import pandas as pd
import yaml


def kws(record, props, patterns):
    """Assign categories and drop columns"""
    text = "".join(record[prop] for prop in props if record.get(prop))
    categories = []

    for category, pattern in patterns.items():
        if re.findall(pattern, text, flags=re.IGNORECASE):
            categories.append(category)

    return {
        "created_utc": record["created_utc"],
        "subreddit": record["subreddit"],
        "cat": categories,
    }


def process(jsonl, patterns, props):
    """Subreddit data processing pipeline.

    Use bag for initial keyword calculation and filtering, was getting
    type errors when using dd.read_json. See this link for more
    information: https://stackoverflow.com/q/54992783/4501508).

    """

    bag = db.read_text(jsonl, blocksize="10MiB").map(json.loads)
    df = bag.map(kws, props, patterns).to_dataframe()

    df["created_utc"] = dd.to_datetime(df["created_utc"], unit="s")
    df = df.set_index("created_utc")

    counts = df.groupby([pd.Grouper(freq="1d"), "subreddit"]).size().to_frame("totals")

    df = df.explode("cat")
    df["cat"] = df["cat"].fillna("nocat").astype("category").cat.as_known()

    agg = (
        df.groupby([pd.Grouper(freq="1d"), "subreddit", "cat"])
        .size()
        .to_frame("count")
        .reset_index()
        .join(counts, on=["created_utc", "subreddit"])
    )

    agg["freq"] = agg["count"] / agg["totals"]

    return agg


def main(termf, subsf, comsf, subprops, comprops, subsout, comsout):
    """Run everything, change dask config if multiprocessing is causing problems"""

    with open(termf) as f:
        terms = yaml.load(f, Loader=yaml.Loader)
        patterns = {cat: "|".join(kw) for cat, kw in terms.items()}

    with dask.config.set(scheduler="processes"):
        subs = process(subsf, patterns, subprops)
        subs.to_csv(subsout, index=False, single_file=True)

        coms = process(comsf, patterns, comprops)
        coms.to_csv(comsout, index=False, single_file=True)


if __name__ == """__main__""":
    main(
        termf="config/coronaterms.yaml",
        subsf="data/subs.jsonl",
        comsf="data/coms.jsonl",
        subprops=["title", "selftext"],
        comprops=["body"],
        subsout="data/subs_agg.csv",
        comsout="data/coms_agg.csv",
    )
