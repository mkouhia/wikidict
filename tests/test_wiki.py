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
        self.wiki_downloader.get_page_list(session, query_from='A Clash of Kings-Chapter 1', max_pages=4)
        self.assertEqual(
            {'A Clash of Kings-Chapter 1', 'A Clash of Kings-Chapter 10', 'A Clash of Kings-Chapter 11',
             'A Clash of Kings-Chapter 12'},
            {p.title for p in session.query(WikiPage)})

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

    def test_update_pages_follow_redirects(self):
        self.wiki_downloader.update_pages(session, page_titles=['Abel'])
        self.assertIsNotNone(session.query(WikiPage).filter(WikiPage.title == 'Mance Rayder').first())

    def test_link_redirects(self):
        self.wiki_downloader.update_pages(session, page_titles=['Abel', 'Aegon I'], follow_redirects=False)
        source_ids = []

        for p in session.query(WikiPage):
            self.assertIsNone(p.redirect_to_id)
            source_ids.append(p.id)

        self.wiki_downloader.update_pages(session, page_titles=['Mance Rayder', 'Aegon I Targaryen'], follow_redirects=False)

        self.wiki_downloader.link_redirects(session, source_ids)

        for source_id in source_ids:
            p = session.query(WikiPage).get(source_id)
            self.assertIsNotNone(p.redirect_to_id)

    def test_update_only_outdated(self):
        page_titles = ['A Rose of Gold', 'A Thousand Eyes, and One', 'A World of Ice and Fire']

        # 1. Download some pages
        self.wiki_downloader.update_pages(session, page_titles=page_titles)
        page_id = session.query(WikiPage)\
            .filter(WikiPage.title == 'A World of Ice and Fire').first().id

        # 2. Set older revision id for one page
        session.query(WikiPage) \
            .filter(WikiPage.title == 'A World of Ice and Fire')\
            .update({'revision_id': (WikiPage.latest_revision_online - 1)})
        session.commit()

        # 3. Get revision IDs for all pages in database
        self.wiki_downloader.update_latest_revisions(session, page_titles=page_titles)

        # 4. See that only this one page needs to be downloaded
        self.assertEqual(
            ['A World of Ice and Fire'],
            [p.title for p in session.query(WikiPage).filter(WikiPage.revision_id < WikiPage.latest_revision_online)]
        )

        # 5. See that only this one page is downloaded
        self.assertEqual([page_id], self.wiki_downloader.update_outdated_pages(session))

        # 6. See that the page has been updated in database
        self.assertIsNone(session.query(WikiPage).filter(WikiPage.revision_id < WikiPage.latest_revision_online).first())
