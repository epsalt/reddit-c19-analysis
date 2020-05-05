"""pushshift.py download subreddit data with the pushshift api"""

from datetime import datetime
import os
from urllib.parse import urlencode

import jsonlines
from ratelimit import limits, sleep_and_retry
import requests


def get_subs(subreddits, before, after, outfile):
    """Download all submissions for subreddits between before and after"""
    before, after = (int(t.timestamp()) for t in (before, after))
    ids = []

    for subreddit in subreddits:
        params = {"subreddit": subreddit, "size": 500, "before": before}
        data = request(params, "submission", after, outfile)

        while data:
            ids.extend(sub["id"] for sub in data)
            params["before"] = data[-1]["created_utc"]
            data = request(params, "submission", after, outfile)

    return ids


def get_coms(ids, before, after, outfile):
    """Download all comments for submission ids between before and after"""
    before, after = (int(t.timestamp()) for t in (before, after))
    id_chunks = chunks(ids, 100)

    for chunk in id_chunks:
        params = {"size": 500, "link_id": ",".join(chunk), "before": before}
        data = request(params, "comment", after, outfile)

        while data:
            params["before"] = data[-1]["created_utc"]
            data = request(params, "comment", after, outfile)


@sleep_and_retry
@limits(calls=2, period=1)
def request(params, endpoint, after, outfile):
    """Make request, filter, then save and return data"""

    template = f"https://api.pushshift.io/reddit/{endpoint}/search/?"
    req = requests.get(template + urlencode(params))
    req.raise_for_status()

    data = req.json()["data"]
    filtered = [item for item in data if item["created_utc"] > after]

    print(
        f"endpoint: {endpoint}, req code: {req.status_code}, "
        f"data: {len(data)}/{len(filtered)}"
    )

    if not filtered:
        return None

    with jsonlines.open(outfile, "a") as writer:
        for item in filtered:
            writer.write(item)

    return filtered


def chunks(lst, n):
    """Slice a list into chunks"""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def main(subsout, comsout, date_range, subreddits):
    """Download data for subreddit submissions and comments"""

    for out in (subsout, comsout):
        os.makedirs(os.path.dirname(out), exist_ok=True)
        if os.path.exists(out):
            raise FileExistsError(f"{out} already exists")

    after, before = date_range
    ids = get_subs(subreddits, before, after, subsout)
    get_coms(ids, before, after, comsout)


if __name__ == """__main__""":
    main(
        subsout="data/subs_may.jsonl",
        comsout="data/coms_may.jsonl",
        date_range=(datetime(2020, 1, 1), datetime(2020, 5, 1)),
        subreddits=["calgary", "edmonton", "alberta"],
    )
