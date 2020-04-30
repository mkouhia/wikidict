import json
from unittest import TestCase

from mediawiki import MediaWiki, MediaWikiException

from wikidict.model import WikiPage, Base
from wikidict.wiki import WikiDownloader

from tests import session, engine


class Test(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        wiki = MediaWiki('http://awoiaf.westeros.org/api.php')
        cls.wiki_downloader = WikiDownloader(wiki)

    def setUp(self) -> None:
        session.expunge_all()
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

    def test_get_pages(self):
        pages = self.wiki_downloader.get_pages(query_from='A Clash of Kings-Chapter 1', max_pages=4)
        self.assertEqual(
            ['A Clash of Kings-Chapter 1', 'A Clash of Kings-Chapter 10', 'A Clash of Kings-Chapter 11',
             'A Clash of Kings-Chapter 12'],
            [p.title for p in pages])

    def test_update_latest_revisions(self):
        page_ids = [2581, 14424, 2752]
        self.wiki_downloader.update_latest_revisions(session, page_ids=page_ids)

        self.assertEqual(3, session.query(WikiPage).count())

        for page in session.query(WikiPage).all():
            self.assertLess(1, page.latest_revision_online)
            self.assertIsNone(page.revision_id)

    def test_update_pages_pageids(self):
        page_ids = [2581, 14424, 2752]
        self.wiki_downloader.update_pages(session, page_ids=page_ids)

        for page_id in page_ids:
            page = session.query(WikiPage).get(page_id)
            self.assertTrue(len(page.content) > 0)

    def test_update_outdated_pages(self):
        page_ids = [2581, 14424, 2752]

        self.wiki_downloader.update_latest_revisions(session, page_ids=page_ids)
        self.wiki_downloader.update_outdated_pages(session)

        session.expunge_all()

        for page_id in page_ids:
            page = session.query(WikiPage).get(page_id)
            self.assertTrue(len(page.content) > 0)

    def test__continued_response(self):
        iterator = self.wiki_downloader._continued_response(
            query_params={'action': 'blah'}, result_parse_func=lambda: None)
        with self.assertRaises(MediaWikiException):
            list(iterator)
