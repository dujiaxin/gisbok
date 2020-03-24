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
from multiprocessing.dummy import Pool
from operator import or_
from pathlib import Path

# Others
from httpx import Client
from lxml.html import fromstring
from tqdm import tqdm

# Types
from typing import Set

DATA_PATH = Path("./data")
URL_BASE = "https://gistbok.ucgis.org"
PAGES = 30


def fetch(client: Client, url: str) -> str:
    """Fetch url and the response text back."""
    return client.get(url).text


def parse_links(text: str) -> Set[str]:
    """Get links from topics' html."""
    return set(map(lambda href: href, fromstring(text).xpath("//h5/a/@href")))


def fetch_all_topics() -> Set[str]:
    with Client() as client:
        with Pool() as pool:
            return reduce(
                or_,
                pool.map(
                    lambda page: parse_links(
                        fetch(
                            client,
                            f"{URL_BASE}/all-topics?"
                            f"term_node_tid_depth=All&page={page}",
                        )
                    ),
                    range(PAGES),
                ),
            )


def fetch_topic(client: Client, url: str, pbar: tqdm = None) -> None:
    """Fetch and save each topic original html."""
    path = DATA_PATH / "htmls" / Path(url).relative_to("/")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix(".html").open("w") as f:
        f.write(client.get(f"{URL_BASE}{url}").text)
    if pbar:
        pbar.update()


def main() -> None:
    """Main function."""
    with Client() as client:
        with Pool(3) as pool:
            topics = fetch_all_topics()
            pbar = tqdm(total=len(topics))
            pool.map(lambda url: fetch_topic(client, url, pbar), topics)


if __name__ == "__main__":
    main()
