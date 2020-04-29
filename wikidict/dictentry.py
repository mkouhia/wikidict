from __future__ import annotations

from typing import List
import mediawiki

from wikidict.parser import Parser


class DictEntry(object):

    def __init__(self, headword: str, body: str = "", word_info: str = "", variants: List[str] = None):
        self.headword = headword
        self.body = body
        self.word_info = word_info
        if variants is None:
            self.variants = []
        else:
            self.variants = variants

    def __str__(self):
        s = "@ {}\n".format(self.headword)
        if len(self.word_info) > 0:
            s += ": {}\n".format(self.word_info)
        for variant in self.variants:
            s += "& {}\n".format(variant)
        if len(self.body) > 0:
            s += self.body + "\n"
        return s + "\n"
