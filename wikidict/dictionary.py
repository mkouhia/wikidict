from __future__ import annotations

import contextlib
import logging
import sys
from typing import List

from sqlalchemy.orm import Session

from wikidict.model import WikiPage
from wikidict.parser import Parser


logger = logging.getLogger(__name__)


class Dictionary(object):

    def __init__(self, session: Session):
        self.session = session

    def save(self, file_path: str = None, dict_format='kobo'):
        """Save wiki dictionary as text file
        :param file_path: file path, or '-'/None for stdout
        :param dict_format: dictionary format. Allowed values: 'kobo'
        """
        logger.info('Writing output file')
        pages = self.session.query(WikiPage) \
            .filter(WikiPage.redirect_to_id == None).order_by('title')  # noqa: E711

        with self._smart_open(file_path) as f:
            page: WikiPage
            for page in pages:
                f.write(DictEntry.from_wikipage(page).format(dict_format))

    @staticmethod
    @contextlib.contextmanager
    def _smart_open(filename=None):
        if filename and filename != '-':
            fh = open(filename, 'w')
        else:
            fh = sys.stdout

        try:
            yield fh
        finally:
            if fh is not sys.stdout:
                fh.close()


class DictEntry(object):

    def __init__(self, title: str, content: str = "", categories: List[str] = None, variants: List[str] = None):
        self.title = title
        self.content = content
        self.categories = categories
        self.variants = variants

    def format(self, dict_format='kobo') -> str:
        """Format dictionary entry to a string
        :param dict_format: output dictionary format. Allowed values: 'kobo'
        """
        if dict_format == 'kobo':
            return self._format_kobo()
        else:
            raise ValueError('Unknown dictionary format: {}'.format(dict_format))

    def _format_kobo(self):
        body = "" if self.content is None else Parser(self.content)\
                   .remove_templates()\
                   .get_first_section()\
                   .remove_category_links()\
                   .to_markdown()\
                   .content
        s = "@ {}\n".format(self.title)
        if len(self.categories) > 0:
            s += ": {}\n".format(self.categories[0])
        for variant in self.variants:
            s += "& {}\n".format(variant)
        if len(body) > 0:
            s += body + "\n"
        return s + "\n"

    @classmethod
    def from_wikipage(cls, page: WikiPage) -> DictEntry:
        return DictEntry(
            title=page.title,
            content=page.content,
            categories=[c.name for c in page.categories],
            variants=[p.title for p in page.redirect_from])
