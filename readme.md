# COVID-19 Discussion in Albertan Subreddits

This project uses data from the `/r/calgary`, `/r/edmonton`, and
`/r/alberta` subreddits to visualize discussion of topics related to
COVID-19 over time. View a notebook explaining the analysis and
exploring the results on [nbviewer][notebook].

![COVID-19 discussion across albertan subreddits][chart]

| Label | Date      | Event                                   |
|:-----:|:---------:|:----------------------------------------|
| A     |2020-01-15 | Canada's first case                     |
| B     |2020-03-05 | Alberta's first case                    |
| C     |2020-03-17 | Canada Declares Public Health Emergency |

## Running the notebook interactively

To run the notebook locally, follow these steps:

```bash
# Clone the repo
$ git clone https://github.com/epsalt/reddit-c19-analysis
$ cd reddit-c19-analysis

# Install dependencies
$ pip install -r requirements.txt

# Download comment data
$ curl https://alberta-reddit-data.s3-us-west-2.amazonaws.com/coms.jsonl.gz -o coms.jsonl.gz
$ gunzip -d coms.jsonl.gz -c > data/coms.jsonl

# Or use `pushshift.py` to request data from the pushshift API 
# - Requests are rate limited so this can take a while
# - Date ranges or subreddits can be changed in the source
$ python pushshift.py

# Run the notebook
$ jupyter notebook c19-reddit-alberta.ipynb
```

[chart]: assets/chart.png
[notebook]: https://nbviewer.jupyter.org/github/epsalt/reddit-c19-analysis/blob/master/c19-reddit-alberta.ipynb
