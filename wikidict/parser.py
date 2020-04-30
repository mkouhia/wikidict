from __future__ import annotations

import re

import mwparserfromhell


class Parser(object):
    template_start_delimiter = '{{'
    template_stop_delimiter = '}}'

    def __init__(self, content):
        self._content = content

    @property
    def content(self):
        return self._content

    def to_markdown(self) -> Parser:
        self._content = self._content \
            .replace("'''", '**') \
            .replace("''", '*')
        self._content = self.__replace_links(self._content)
        return self

    @classmethod
    def __replace_links(cls, content: str) -> str:
        while True:
            match = re.search('\\[\\[([^|\\]]+)(?:\\|([^\\]]+))?\\]\\]', content)
            if match is None:
                return content
            target = match.group(1)
            name = match.group(2) or target
            link = '[{}](#{})'.format(name, target.replace(' ', '-').lower())
            content = content[:match.start()] + link + content[match.end():]

    def get_first_section(self) -> Parser:
        """Get cleaned-up first section from MediaWiki wikitext content
        :return: first section, with templates removed
        """
        wiki_code = mwparserfromhell.parse(self._content)
        sections = wiki_code.get_sections()
        self._content = sections[0].strip() if len(sections) > 0 else ''
        return self

    def remove_templates(self) -> Parser:
        """Remove MediaWiki wikitext templates from content string
        :return: content, with templates removed
        """
        self._content = self._remove_templates(self._content)
        return self

    def remove_category_links(self) -> Parser:
        self._content = re.sub('\\[\\[Category:[^\\]]+\\]\\]', '', self._content)
        return self

    @classmethod
    def _remove_templates(cls, content) -> str:
        while True:
            start_idx = content.find(cls.template_start_delimiter)
            stop_idx = content.find(cls.template_stop_delimiter, start_idx + len(cls.template_start_delimiter))
            if start_idx == -1 or stop_idx == -1:
                return content

            next_start_idx = content.find(cls.template_start_delimiter, start_idx + len(cls.template_start_delimiter))
            if next_start_idx != -1 and next_start_idx < stop_idx:
                # Nested content found
                content = content[:next_start_idx] + \
                          cls._remove_templates(content[next_start_idx:stop_idx + len(cls.template_stop_delimiter)]) + \
                          content[stop_idx + len(cls.template_stop_delimiter):]
                continue

            content = content[:start_idx] + content[stop_idx + len(cls.template_stop_delimiter):]
