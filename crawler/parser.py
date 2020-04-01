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
from csv import DictWriter
from dataclasses import dataclass
from functools import wraps
from pathlib import Path

# Others
from lxml.etree import _ElementTree as ET  # noqa: WPS436
from lxml.html import parse as html_parse  # noqa: WPS347
from tqdm import tqdm

# Types
from typing import Callable, Sequence, Tuple, TypeVar, Union

a = TypeVar("a")
TS = Tuple[str, ...]

EMPTY = ""
DATA_PATH = Path("./data")
HTML_TOPIC_PATH = DATA_PATH / "htmls" / "bok-topics"


def to_content(path: str) -> str:
    """Change back to content from path string."""
    return path.replace("|--|", ":").replace("|-|", "/")


def clean(text: str) -> str:
    """Clean text."""
    return text.strip().replace("\xa0", " ")


def cleand(func: Callable[[ET], str]) -> Callable[[ET], str]:
    """Clean text decorator."""

    @wraps(func)
    def wrapper(etree: ET) -> str:
        """Wrap the clean func."""
        return clean(func(etree))

    return wrapper


def cleantd(func: Callable[[ET], TS]) -> Callable[[ET], TS]:
    """Clean text decorator."""

    @wraps(func)
    def wrapper(etree: ET) -> TS:
        """Wrap the clean tuple func."""
        return tuple(map(clean, func(etree)))

    return wrapper


def text_only(etrees: Sequence[ET]) -> TS:
    """Text only of `etrees`."""
    return tuple(map(lambda etree: etree.text_content(), etrees))


def text_onlyd(
    func: Callable[[Sequence[ET]], Sequence[ET]]
) -> Callable[[Sequence[ET]], TS]:
    """Text only decorator."""

    @wraps(func)
    def wrapper(etrees: ET) -> TS:
        """Wrap the text_only func."""
        return text_only(func(etrees))

    return wrapper


def first(element: Sequence[a]) -> Union[a, str]:
    """First element."""
    return element and element[0] or EMPTY


def firstd(func: Callable[[ET], Sequence[a]]) -> Callable[[ET], a]:
    """First element decorator."""

    @wraps(func)
    def wrapper(etree: ET) -> Union[a, str]:
        """Wrap the first func."""
        return first(func(etree))

    return wrapper


@dataclass
class Topic:
    """Topic data module."""

    # body: str
    doi: str
    shortlink: str
    canonical: str
    topic: str
    area: str
    theme: str
    author: str
    abstract: str
    keywords: TS
    learning_objectives: TS
    related_topics: TS
    references: TS
    additional_resources: Tuple[Tuple[str, str], ...]
    instructional_assessment_questions: TS

    @classmethod
    def from_etree(
        cls,
        etree: ET,
        *,
        topic: str = EMPTY,
        theme: str = EMPTY,
        area: str = EMPTY,
    ) -> Topic:
        """Initialize function from etree."""
        return cls(
            parse_doi(etree),
            parse_shortlink(etree),
            parse_canonical(etree),
            to_content(topic),
            to_content(area),
            to_content(theme),
            parse_authors(etree),
            parse_abstract(etree),
            parse_keywords(etree),
            parse_learning_objectives(etree),
            parse_related_topics(etree),
            parse_references(etree),
            parse_additional_resources(etree),
            parse_instructional_assessment_questions(etree),
        )

    @classmethod
    def from_path(cls, path: Union[Path, str]) -> Topic:
        """Initialize function from path."""
        return (
            lambda parts: cls.from_etree(
                html_parse(str(path)),
                topic=parts[2],
                area=parts[0],
                theme=parts[1],
            )
        )(Path(path).stem.split(" || "))


@cleand
def parse_body(etree: ET) -> str:
    """Parse all content."""
    return EMPTY.join(
        etree.xpath("//div[contains(@class, 'node-content')]//text()")
    )


@cleand
@firstd
def parse_doi(etree: ET) -> str:
    """Parse DOI."""
    return etree.xpath(
        "//*[@id='info']//a[contains(@href, 'doi.org')]//text()"
    )


@firstd
def parse_shortlink(etree: ET) -> str:
    """Parse shortlink."""
    return etree.xpath("//link[contains(@rel, 'shortlink')]/@href")


@firstd
def parse_canonical(etree: ET) -> str:
    """Parse canonical."""
    return etree.xpath("//link[contains(@rel, 'canonical')]/@href")


@cleand
@firstd
def parse_abstract(etree: ET) -> str:
    """Parse abstract."""
    return etree.xpath(
        "//div[contains(@class, 'field-type-text-with-summary')]//p//text()"
    )


@cleand
@firstd
@text_onlyd
def parse_authors(etree: ET) -> str:
    """Parse all authors."""
    return etree.xpath("//div[@id='info']")


@cleantd
def parse_keywords(etree: ET) -> TS:
    """Parse tuple of keywords."""
    return etree.xpath("//*[@id='keywords']//li//text()")


@cleantd
@text_onlyd
def parse_learning_objectives(etree: ET) -> TS:
    """Parse tuple of learning objectives."""
    return etree.xpath(
        "//div[contains(@class, 'field-name-field-learning-objectives')]"
        "//li"
    )


@cleantd
def parse_related_topics(etree: ET) -> TS:
    """Parse tuple of related topics."""
    return etree.xpath("//*[@id='related-topics']//a//@href")


@cleantd
def parse_topic_description(etree: ET) -> TS:
    """Parse tuple of topic description."""
    return etree.xpath("//*[@id='toc']//ol//a//text()")


@cleantd
@text_onlyd
def parse_references(etree: ET) -> TS:
    """Parse tuple of reference."""
    return etree.xpath("//*[@id='bibliography']//p")


def parse_additional_resources(etree: ET) -> Tuple[Tuple[str, str], ...]:
    """Parse tuple of additional resources."""
    return tuple(
        map(
            lambda et: (
                clean(et.text_content()),
                first(et.xpath(".//a/@href")),
            ),
            etree.xpath("//*[@id='additional-resources']//p"),
        )
    )


@cleantd
@text_onlyd
def parse_instructional_assessment_questions(etree: ET) -> TS:
    """Parse tuple of instructional assessment questions."""
    return etree.xpath(
        "//div[contains(@class, 'field-name-field-learning-questions')]"
        "//div[contains(@class, 'even')]/ol/li"
    )


def main() -> None:
    """Run main function."""
    # Write topic, theme, area and learning objectives
    with open("sample.csv", "w", encoding="utf-8") as f:
        writer = DictWriter(
            f, fieldnames=["topic", "theme", "area", "learning_objective"]
        )
        writer.writeheader()
        for path in tqdm(HTML_TOPIC_PATH.glob("*.html")):
            topic = Topic.from_path(path)
            if topic.learning_objectives:
                for l_o in topic.learning_objectives:
                    writer.writerow(
                        {
                            "topic": topic.topic,
                            "theme": topic.theme,
                            "area": topic.area,
                            "learning_objective": l_o,
                        }
                    )
            else:
                writer.writerow(
                    {
                        "topic": topic.topic,
                        "theme": topic.theme,
                        "area": topic.area,
                        "learning_objective": "",
                    }
                )

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
