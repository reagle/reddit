#!/usr/bin/env python3
"""Web utility functions."""

__author__ = "Joseph Reagle"
__copyright__ = "Copyright (C) 2020-2023 Joseph Reagle"
__license__ = "GLPv3"
__version__ = "1.0"

import contextlib as cl
import html.entities
import json
import logging
import os
import re
import time
from typing import Any
from xml.sax import saxutils

import cachier
import lxml
import Path
import requests  # http://docs.python-requests.org/en/latest/

HOMEDIR = Path.home()


log = logging.getLogger("web_utils")


def escape_XML(text: str) -> str:  # http://wiki.python.org/moin/EscapingXml
    """Escape XML character entities; & < > are defaulted."""
    extras = {"\t": "  "}
    return saxutils.escape(text, extras)


def unescape_XML(text: str) -> str:  # .0937s 4.11%
    """Remove HTML or XML character references and entities from text.

    http://effbot.org/zone/re-sub.htm#unescape-htmlentitydefs

    Marginally faster than `from xml.sax.saxutils import escape, unescape`.

    """

    def fixup(m: re.Match):
        text = m.group(0)
        if text[:2] == "&#":
            # character reference
            with cl.suppress(ValueError):
                if text[:3] == "&#x":
                    return chr(int(text[3:-1], 16))
                else:
                    return chr(int(text[2:-1]))
        else:
            # named entity
            with cl.suppress(KeyError):
                text = chr(html.entities.name2codepoint[text[1:-1]])
        return text  # leave as is

    return re.sub(r"&#?\w+;", fixup, text)


@cachier.cachier(pickle_reload=False)  # stale_after=dt.timedelta(days=7)
def get_HTML(
    url: str,
    referer: str = "",
    data: str = "",
    cookie: str = "",
    retry_counter: int = 0,
    rate_limit: int = 2,
    cache_control: str = "",
) -> tuple[bytes, Any, str, requests.models.Response]:
    """Return [HTML content, response] of a given URL."""
    time.sleep(rate_limit)

    AGENT_HEADERS = {"User-Agent": "MacOS:reddit-query.py:v0.5 (by /u/reagle-reseach)"}
    r = requests.get(url, headers=AGENT_HEADERS, verify=True)
    # info(f"{r.headers['content-type']=}")
    if "html" in r.headers["content-type"]:
        HTML_bytes = r.content
    else:
        raise OSError("URL content is not HTML.")

    parser_html = lxml.etree.HTMLParser()  # type: ignore
    doc = lxml.etree.fromstring(HTML_bytes, parser_html)  # type: ignore
    HTML_parsed = doc

    HTML_utf8 = lxml.etree.tostring(HTML_parsed, encoding="utf-8")  # type: ignore
    HTML_unicode = HTML_utf8.decode("utf-8", "replace")

    return HTML_bytes, HTML_parsed, HTML_unicode, r


@cachier.cachier(pickle_reload=False)  # stale_after=dt.timedelta(days=7)
def get_JSON(
    url: str,
    referer: str = "",
    data: str = "",
    cookie: str = "",
    retry_counter: int = 0,
    rate_limit: int = 2,
    cache_control: str = "",
    requested_content_type: str = "application/json",
) -> list | dict:  # different services return [... or {...
    """Return [JSON content, response] of a given URL.

    Default rate limit is 2 seconds per request, though Pushshift
    can limit me down to 3 minutes!
    https://www.reddit.com/r/pushshift/comments/shg1sy/rate_limit/
    """
    time.sleep(rate_limit)

    # TODO: put limiter here? https://github.com/shaypal5/cachier/issues/65
    AGENT_HEADERS = {"User-Agent": "Reddit Tools https://github.com/reagle/reddit/"}
    log.info(f"{url=}")
    # TODO: use a HTTPAdapter with max_retires
    # https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/#retry-on-failure

    try:
        r = requests.get(url, headers=AGENT_HEADERS, verify=True)
        r.raise_for_status()
    except requests.exceptions.RequestException as err:
        log.critical(f"{err=} -- waiting 5 minutes, try again, quit if fail")
        time.sleep(300)  # wait 5 minutes
        r = requests.get(url, headers=AGENT_HEADERS, verify=True)
        r.raise_for_status()
    returned_content_type = r.headers["content-type"].split(";")[0]
    log.info(f"{requested_content_type=} == {returned_content_type=}?")
    if requested_content_type == returned_content_type:
        json_content = json.loads(r.content)
        return json_content
    else:
        raise OSError("URL content is not JSON.")


@cachier.cachier(pickle_reload=False)  # stale_after=dt.timedelta(days=7)
def get_text(url: str) -> str:
    """Textual version of url."""
    return str(os.popen(f'w3m -O utf8 -cols 10000 -dump "{url}"').read())
