#!/usr/bin/env python
# -*- coding: utf-8 -*-

r"""
Life's pathetic, have fun ("▔□▔)/hi~♡ Nasy.

Excited without bugs::

    |             *         *
    |                  .                .
    |           .
    |     *                      ,
    |                   .
    |
    |                               *
    |          |\___/|
    |          )    -(             .              ·
    |         =\ -   /=
    |           )===(       *
    |          /   - \
    |          |-    |
    |         /   -   \     0.|.0
    |  NASY___\__( (__/_____(\=/)__+1s____________
    |  ______|____) )______|______|______|______|_
    |  ___|______( (____|______|______|______|____
    |  ______|____\_|______|______|______|______|_
    |  ___|______|______|______|______|______|____
    |  ______|______|______|______|______|______|_
    |  ___|______|______|______|______|______|____

author   : Nasy https://nasy.moe
date     : Mar 20, 2020
email    : Nasy <nasyxx+python@gmail.com>
filename : spider.py
project  : crawler
license  : GPL-3.0+

At pick'd leisure
  Which shall be shortly, single I'll resolve you,
Which to you shall seem probable, of every
  These happen'd accidents
                          -- The Tempest
"""

# Standard Library
from functools import reduce
from itertools import chain, zip_longest
from multiprocessing.dummy import Pool
from operator import or_
from pathlib import Path

# Others
from httpx import Client
from lxml.etree import _ElementTree as ET  # noqa: WPS436
from lxml.html import fromstring
from tqdm import tqdm

# Types
from typing import List, Set, Tuple

DATA_PATH = Path("./data")
URL_BASE = "https://gistbok.ucgis.org"
PAGES = (90, 100)
KAS = (
    "Foundational Concepts",
    "Knowledge Economy",
    "Computing Platforms",
    "Programming and Development",
    "Data Capture",
    "Data Management",
    "Analytics and Modeling",
    "Cartography and Visualization",
    "Domain Applications",
    "GIS&T and Society",
)

EMPTY = ""


def fetch(client: Client, url: str) -> str:
    """Fetch url and the response text back."""
    return client.get(url).text


def build_topics(
    topics: List[Tuple[str, str, str]], iele: Tuple[int, ET]
) -> List[Tuple[str, str, str]]:
    """Build topic."""
    return (
        lambda theme, text_href: topics + [(theme, text_href[0], text_href[1])]
    )(
        (
            lambda td: td
            and str(td[0].text_content())
            .strip()
            .replace("/", "|-|")
            .replace(":", "|--|")
            or topics[-1][0]
        )(
            iele[1].xpath(
                f"./td[{iele[0]}][not(contains(@class, 'rteright'))]/*"
            )
        ),
        (
            lambda td: td
            and (
                str(td[0].text_content())
                .strip()
                .replace("/", "|-|")
                .replace(":", "|--|"),
                str(
                    td[0]
                    .attrib.get("href", EMPTY)
                    .replace("//10.22224", "//doi.org/10.22224")
                ),
            )
            or (EMPTY, EMPTY)
        )(iele[1].xpath(f"./td[{iele[0]}][contains(@class, 'rteright')]/*")),
    )


def parse_links(text: str, ka: str) -> Set[Tuple[str, str, str, str]]:
    """Get links from topics' html."""
    return set(
        map(
            lambda topic: (ka, topic[0] or topic[1], topic[1], topic[2]),
            filter(
                lambda topic: topic[1],
                (
                    lambda trs: reduce(
                        build_topics,
                        chain.from_iterable(
                            map(
                                lambda col: zip_longest(
                                    [], trs, fillvalue=col
                                ),
                                range(1, 4),
                            )
                        ),
                        [(EMPTY, EMPTY, EMPTY)],
                    )
                )(fromstring(text).xpath("//tr")),
            ),
        ),
    )


def fetch_all_topics() -> Set[Tuple[str, str]]:
    """Fetch all topics."""
    with Client() as client:
        with Pool() as pool:
            return reduce(
                or_,
                pool.map(
                    lambda page_ka: parse_links(
                        fetch(
                            client,
                            f"{URL_BASE}/all-topics?"
                            f"term_node_tid_depth={page_ka[0]}",
                        ),
                        page_ka[1],
                    ),
                    zip(range(*PAGES), KAS),
                ),
            )


def fetch_topic(
    client: Client, topic: Tuple[str, str], pbar: tqdm = None
) -> None:
    """Fetch and save each topic original html."""
    path = DATA_PATH / "htmls" / "bok-topics" / Path(" || ".join(topic[:-1]))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix(".html").open("wb") as f:
        f.write(topic[-1] and client.get(topic[-1]).content or b"empty")
    if pbar:
        pbar.update()


def main() -> None:
    """Run Crawler."""
    with Client() as client:
        with Pool(3) as pool:
            topics = fetch_all_topics()
            pbar = tqdm(total=len(topics))
            pool.map(lambda url: fetch_topic(client, url, pbar), topics)


