#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Copyright 2020 by Joseph Reagle
# Licensed under the GPLv3, see <http://www.gnu.org/licenses/gpl-3.0.html>
# Given a spreadsheet with phrases, facilitate a search of those
# appearing in a column by opening browser windows to relevant search engines.

"""Facilitate a search of phrases appearing in a
spreadsheet column (default: 'original') by generating queries against
search engines and opening the results in browser tabs. Search engines
include Google, Reddit, and RedditSearch/Pushshift."""

import argparse  # http://docs.python.org/dev/library/argparse.html
import logging
import sys
import webbrowser
from os import name, system
from pathlib import Path  # https://docs.python.org/3/library/pathlib.html

# import urllib

import pandas as pd

import requests

# from phantomjs import Phantom # TODO: use this for searching DOM?


HOME = str(Path("~").expanduser())

exception = logging.exception
critical = logging.critical
error = logging.error
warning = logging.warning
info = logging.info
debug = logging.debug

HEADERS = {"User-Agent": "Reddit Search https://github.com/reagle/reddit"}


def auto_search(
    query: str, subreddit: str, quote: str, target_url: str
) -> None:
    """Does the URL appear in the query results?"""

    info(f"{subreddit=}")
    info(f"{query=}")
    if not target_url:  # there's nothing to test against in results
        print("auto_search: N/A")
        return

    if "redditsearch.io" in query:  # use pushshift for auto_search
        query = (
            "https://api.pushshift.io/reddit/submission/search/"
            "?subreddit={subreddit}&q={quote}"
        )

    query_inexact = query.format(subreddit=subreddit, quote=quote)
    info(f"{query_inexact=}")
    response = requests.get(query_inexact, headers=HEADERS)

    # remove url scheme and netloc from https://old.reddit.com/...
    target_url = "/".join(target_url.split("/")[3:])  # TODO use urllib.parse
    # if reddit, just check on t3 message ID and title
    if target_url.startswith("r/"):
        target_url = "/".join(target_url.split("/")[3:5])
    if target_url in response.text:
        print(
            f"auto_search: found inexact at "
            f"{query_inexact.replace(' ', '+')}"  # TODO: properly quote
        )
        return

    # Google does well with exact searches, so run again as exact
    if "google.com" in query:
        info("google query EXACT")
        info(f"{query=}")
        query_exact = query.format(subreddit=subreddit, quote=f'"{quote}"')
        info(f"{query_exact=}")
        response = requests.get(query_exact, headers=HEADERS)
        if target_url in response.text:
            print(f"auto_search: found   exact at {query_exact[0:30]}")
            return


def quotes_search(row: dict, heading: str, do_recheck: bool) -> None:
    """Open browser on each search engine to help find quotes."""

    system("cls") if name == "nt" else system("clear")
    if row[heading] == "" or "found" not in row:
        return
    info(f"{row['found']=}")
    if do_recheck or row["found"] == "" or pd.isnull(row["found"]):
        info(f"checking")
        quote = row[heading]
        print(f"{quote}\n")
        if row["subreddit"]:
            subreddit = f"{row['subreddit']}"
        else:
            subreddit = ""
        debug("-------------------------")
        debug(f"{row['original']}\n")

        # Google query
        query_google = (
            """https://www.google.com/search"""
            """?q=site:reddit.com r/{subreddit} {quote}"""
        )
        auto_search(query_google, subreddit, quote, row["url"])
        query_google_final = query_google.format(
            subreddit=subreddit, quote=quote
        )
        debug(f"Google query:       {query_google_final}")
        webbrowser.open(query_google_final)

        # Reddit query
        query_reddit = (
            """https://old.reddit.com/r/{subreddit}/search/"""
            """?q={quote}&restrict_sr=on&include_over_18=on"""
        )
        auto_search(query_reddit, subreddit, quote, row["url"])
        query_reddit_final = query_reddit.format(
            subreddit=subreddit, quote=quote
        )
        debug(f"Reddit query:       {query_reddit_final}")
        webbrowser.open(query_reddit_final)

        # RedditSearch (Pushshift)
        query_pushshift = (
            """https://redditsearch.io/"""
            """?term={quote}&dataviz=false&aggs=false"""
            """&subreddits={subreddit}&searchtype=posts,comments"""
            """&search=true&start=0&end=1594758200&size=100"""
        )
        auto_search(query_pushshift, subreddit, quote, row["url"])
        query_pushshift_final = query_pushshift.format(
            subreddit=subreddit, quote=quote
        )
        debug(f"Pushshift query:       {query_pushshift_final}")
        webbrowser.open(query_pushshift_final)

        character = input("\n`enter` to continue | `q` to quit: ")

        if character == "q":
            sys.exit()


def grab_quotes(file_name: str, column: str, do_recheck: bool) -> None:
    """Read a column of quotes from a spreadsheet."""

    info(f"{file_name=}, {column=}, {do_recheck=}")
    suffix = Path(file_name).suffix
    if suffix in [".xls", ".xlsx", ".odf", ".ods", ".odt"]:
        df = pd.read_excel(file_name, keep_default_na=False)
        # key, subreddit, type, original, found
        # key, subreddit, type, original, spintax, spinrewriter, found
        # key, subreddit, type, original, wordai, found
        for counter, row in df.iterrows():
            print(f"{row=}")
            quotes_search(row, column, do_recheck)

    elif suffix in [".csv"]:
        df = pd.read_csv(file_name, delimiter=",", keep_default_na=False)
        for counter, row in df.iterrows():
            quotes_search(row, column, do_recheck)
    else:
        print(f"{file_name}")
        raise ValueError("unknown file type/extension")
        sys.exit()


def main(argv) -> argparse.Namespace:
    """Process arguments"""
    arg_parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="""        Facilitate a search of phrases appearing in a
        spreadsheet column (default: 'original') by generating queries against
        search engines and opening the results in browser tabs. Search engines
        include Google, Reddit, and RedditSearch/Pushshift.

        > reddit-search.py demo-phrases.csv -c original

        If you wish to test the efficacy of a disguised/spun phrase, also
        include a column of the spun phrase and the 'url' of the source. This
        will automatically check the results for that URL.

        > reddit-search.py demo-phrases.csv -c weakspins
        """,
    )

    # positional arguments
    arg_parser.add_argument(
        "file_name",
        nargs=1,
        metavar="FILE",
    )
    # optional arguments
    arg_parser.add_argument(
        "-r",
        "--recheck",
        action="store_true",
        default=False,
        help="recheck non-NULL values in 'found' column",
    )
    arg_parser.add_argument(
        "-c",
        "--column",
        default="original",
        help="sheet column to query [default: 'original']",
    )
    arg_parser.add_argument(
        "-L",
        "--log-to-file",
        action="store_true",
        default=False,
        help="log to file %(prog)s.log",
    )
    arg_parser.add_argument(
        "-V",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (specify multiple times for more)",
    )
    arg_parser.add_argument("--version", action="version", version="0.2")
    args = arg_parser.parse_args(argv)

    log_level = logging.ERROR  # 40

    if args.verbose == 1:
        log_level = logging.WARNING  # 30
    elif args.verbose == 2:
        log_level = logging.INFO  # 20
    elif args.verbose >= 3:
        log_level = logging.DEBUG  # 10
    LOG_FORMAT = "%(levelname).3s %(funcName).5s: %(message)s"
    if args.log_to_file:
        print("logging to file")
        logging.basicConfig(
            filename="reddit-search.log",
            filemode="w",
            level=log_level,
            format=LOG_FORMAT,
        )
    else:
        logging.basicConfig(level=log_level, format=LOG_FORMAT)

    return args


if __name__ == "__main__":
    args = main(sys.argv[1:])
    debug(f"{args=}")
    grab_quotes(args.file_name[0], args.column, args.recheck)
