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
# Standard Library
import re
from csv import DictWriter
from dataclasses import dataclass
from functools import wraps

# Others
from dataclasses_json import dataclass_json
from lxml.etree import _ElementTree as ET
from lxml.html import parse as html_parse

# Types
from typing import Callable, Set, Tuple, Union


@dataclass
class Author:
    name: str
    # TODO


@dataclass_json
@dataclass(init=False)
class Topic:
    # body: str
    doi: str
    title: str
    abstract: str
    keywords: Tuple[str, ...]
    learning_objectives: Tuple[str, ...]
    related_topics: Tuple[str, ...]

    def __init__(self, etree: ET) -> None:
        """Initial function."""
        self.body = parse_body(etree)
        self.doi = parse_doi(etree)
        self.title = parse_title(etree)
        self.abstract = parse_abstract(etree)
        self.keywords = parse_keywords(etree)
        self.learning_objectives = parse_learning_objectives(etree)
        self.related_topics = parse_related_topics(etree)


def clean(text: str) -> str:
    """Clean text."""
    return text.strip().replace("\xa0", " ")


def cleand(func: Callable[[ET], str]) -> Callable[[ET], str]:
    """Clean text decorator."""

    @wraps(func)
    def wrapper(etree: ET) -> str:
        """Wrapper of func."""
        return clean(func(etree))

    return wrapper


def cleantd(
    func: Callable[[ET], Tuple[str, ...]]
) -> Callable[[ET], Tuple[str, ...]]:
    """Clean text decorator."""

    @wraps(func)
    def wrapper(etree: ET) -> Tuple[str, ...]:
        """Wrapper of func."""
        return tuple(map(clean, func(etree)))

    return wrapper


@cleand
def parse_body(etree: ET) -> str:
    """Parse all content."""
    return "".join(
        etree.xpath("//div[contains(@class, 'node-content')]//text()")
    )


@cleand
def parse_doi(etree: ET) -> str:
    """Parse DOI."""
    return etree.xpath(
        "//*[@id='info']//a[contains(@href, 'doi.org')]//text()"
    )[0]


@cleand
def parse_title(etree: ET) -> str:
    """Parse title."""
    return etree.xpath("//*[@id='page-title']/text()")[0]


@cleand
def parse_abstract(etree: ET) -> str:
    """Parse abstract."""
    return etree.xpath(
        "//div[contains(@class, 'field-type-text-with-summary')]//p//text()"
    )[0]


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
def parse_learning_objectives(etree: ET) -> Tuple[str, ...]:
    """Parse tuple of learning objectives."""
    return etree.xpath("//*[@id='ins-resources']//li//text()")


@cleantd
def parse_related_topics(etree: ET) -> Tuple[str, ...]:
    """Parse tuple of related topics."""
    return etree.xpath("//*[@id='related-topics']//a//@href")


@cleantd
def parse_topic_description(etree: ET) -> Tuple[str, ...]:
    """Parse tuple of topic description."""
    return etree.xpath("//*[@id='toc']//ol//a//text()")


if __name__ == "__main__":
    topic = Topic(
        html_parse(
            "./data/htmls/bok-topics/academic-developments-gist-english-speaking-countries-partial-history.html"
        )
    )
    with open("sample.csv", "w") as f:
        writer = DictWriter(f, fieldnames=list(topic.to_dict().keys()))
        writer.writeheader()
        writer.writerow(topic.to_dict())
