#!/usr/bin/env python3
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
date     : Mar 21, 2020
email    : Nasy <nasyxx+python@gmail.com>
filename : parser.py
project  : crawler
license  : GPL-3.0+

At pick'd leisure
  Which shall be shortly, single I'll resolve you,
Which to you shall seem probable, of every
  These happen'd accidents
                          -- The Tempest

---

Object hierarchy: BoK > Knowledge Area > Unit > Topic > DOI

Attributes (HasA)

Author(s)

Learning Objectives - individually

References - individually

Abstract

Keywords

Topic Description

Definitions (needs extraction from Topic Description)

The purpose of adding them into an owl file:
"""
from __future__ import annotations

# Standard Library
from csv import DictReader, DictWriter
from dataclasses import dataclass
from functools import wraps
from pathlib import Path

# Others
from lxml.etree import _ElementTree as ET  # noqa pylint: disable=E0611
from lxml.html import parse as html_parse
from tqdm import tqdm

# Types
from typing import Callable, List, Set, Tuple, TypeVar, Union

a = TypeVar("a")

DATA_PATH = Path("./data")
HTML_TOPIC_PATH = DATA_PATH / "htmls" / "bok-topics"


@dataclass
class Author:
    """Author data module."""

    name: str
    # TODO


@dataclass
class Topic:
    """Topic data module."""

    # body: str
    doi: str
    shortlink: str
    canonical: str
    title: str
    abstract: str
    keywords: Tuple[str, ...]
    learning_objectives: Tuple[str, ...]
    related_topics: Tuple[str, ...]

    @classmethod
    def from_etree(cls, etree: ET, *, title: str = "") -> Topic:
        """Initialize function from etree."""
        return cls(
            parse_doi(etree),
            parse_shortlink(etree),
            parse_canonical(etree),
            title or parse_title(etree),
            parse_abstract(etree),
            parse_keywords(etree),
            parse_learning_objectives(etree),
            parse_related_topics(etree),
        )

    @classmethod
    def from_path(cls, path: Union[Path, str]) -> Topic:
        """Initialize function from path."""
        return cls.from_etree(html_parse(str(path)), title=Path(path).stem)


def clean(text: str) -> str:
    """Clean text."""
    return text.strip().replace("\xa0", " ")


def cleand(func: Callable[[ET], str]) -> Callable[[ET], str]:
    """Clean text decorator."""

    @wraps(func)
    def wrapper(etree: ET) -> str:
        """Wrap the func."""
        return clean(func(etree))

    return wrapper


def cleantd(
    func: Callable[[ET], Tuple[str, ...]]
) -> Callable[[ET], Tuple[str, ...]]:
    """Clean text decorator."""

    @wraps(func)
    def wrapper(etree: ET) -> Tuple[str, ...]:
        """Wrap the func."""
        return tuple(map(clean, func(etree)))

    return wrapper


def text_only(etrees: List[ET]) -> Tuple[str, ...]:
    """Text only of `etrees`."""
    return tuple(map(lambda etree: etree.text_content(), etrees))


def text_onlyd(
    func: Callable[[List[ET]], List[ET]]
) -> Callable[[List[ET]], Tuple[str, ...]]:
    """Text only decorator."""

    @wraps(func)
    def wrapper(etrees: ET) -> Tuple[str, ...]:
        """Wrap the func."""
        return text_only(func(etrees))

    return wrapper


def first(element: List[str]) -> str:
    """Clean text."""
    return element and element[0] or ""


def firstd(func: Callable[[ET], List[str]]) -> Callable[[ET], str]:
    """Clean text decorator."""

    @wraps(func)
    def wrapper(etree: ET) -> str:
        """Wrap the func."""
        return first(func(etree))

    return wrapper


@cleand
def parse_body(etree: ET) -> str:
    """Parse all content."""
    return "".join(
        etree.xpath("//div[contains(@class, 'node-content')]//text()")
    )


@cleand
@firstd
def parse_doi(etree: ET) -> List[str]:
    """Parse DOI."""
    return etree.xpath(
        "//*[@id='info']//a[contains(@href, 'doi.org')]//text()"
    )


@firstd
def parse_shortlink(etree: ET) -> List[str]:
    """Parse shortlink."""
    return etree.xpath("//link[contains(@rel, 'shortlink')]/@href")


@firstd
def parse_canonical(etree: ET) -> List[str]:
    """Parse canonical."""
    return etree.xpath("//link[contains(@rel, 'canonical')]/@href")


@cleand
def parse_title(etree: ET) -> str:
    """Parse title."""
    return first(etree.xpath("//title/text()")).spilt("|")[-1]


@cleand
@firstd
def parse_abstract(etree: ET) -> List[str]:
    """Parse abstract."""
    return etree.xpath(
        "//div[contains(@class, 'field-type-text-with-summary')]//p//text()"
    )


def parse_attributes(etree: ET) -> str:
    """Parse attributes."""
    # TODO


def parse_authors(etree: ET) -> Set[Author]:
    """Parse all authors."""
    # etree.xpath("//*[@id='info']//div[contains(@class, 'even')]/p/text()")
    # TODO


@cleantd
def parse_keywords(etree: ET) -> Tuple[str, ...]:
    """Parse tuple of keywords."""
    return etree.xpath("//*[@id='keywords']//li//text()")


@cleantd
@text_onlyd
def parse_learning_objectives(etree: ET) -> List[ET]:
    """Parse tuple of learning objectives."""
    return etree.xpath(
        "//div[contains(@class, 'field-name-field-learning-objectives')]"
        "//li"
    )


@cleantd
def parse_related_topics(etree: ET) -> Tuple[str, ...]:
    """Parse tuple of related topics."""
    return etree.xpath("//*[@id='related-topics']//a//@href")


@cleantd
def parse_topic_description(etree: ET) -> Tuple[str, ...]:
    """Parse tuple of topic description."""
    return etree.xpath("//*[@id='toc']//ol//a//text()")


def main() -> None:
    """Main function."""
    # Write learning objectives
    with open("sample.csv", "w", encoding="utf-8") as f, open(
        "./gisbok_knowledgeArea_result.csv", encoding="utf-8"
    ) as g:
        topics = list(DictReader(g))
        files = tuple(HTML_TOPIC_PATH.glob("*.html"))
        ptopics = [Topic.from_path(path) for path in files]

        writer = DictWriter(
            f, fieldnames=["topic", "theme", "area", "learning_objective"]
        )
        writer.writeheader()

        for topic in tqdm(topics):
            for ptopic in ptopics:
                if topic["topic"] in ptopic.title:
                    for l_o in ptopic.learning_objectives:
                        topic["learning_objective"] = l_o
                        writer.writerow(topic)
                    break

    print(
        "----------------\n"
        "NOTE: Due to there is 'comma(,)' in `learning_objective`, "
        "we cannot directly split it by comma.  Here is an example "
        "to read it.",
    )
    print(
        """
from csv import DictReader
with open("sample.csv", encoding="utf-8") as f:
     for line in DictReader(f):  # line is a dict with key of
        print(line)  # ["topic", "theme", "area", "learning_objective"]""",
    )


if __name__ == "__main__":
    main()
