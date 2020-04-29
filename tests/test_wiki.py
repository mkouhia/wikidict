from unittest import TestCase

from mediawiki import MediaWiki
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from wikidict.model import WikiPage, Base
from wikidict.wiki import get_pages, update_latest_revisions


class Test(TestCase):
    wiki = MediaWiki('http://awoiaf.westeros.org/api.php')
    engine = create_engine('sqlite://', echo=True)

    def setUp(self) -> None:
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

    @classmethod
    def setUpClass(cls) -> None:
        Session = sessionmaker(bind=cls.engine)
        cls.session = Session()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.session.close()

    def test_get_pages(self):
        pages = get_pages(wiki=self.wiki, query_from="A Clash of Kings-Chapter 1", max_pages=4)
        self.assertEqual(
            ["A Clash of Kings-Chapter 1", "A Clash of Kings-Chapter 10", "A Clash of Kings-Chapter 11",
             "A Clash of Kings-Chapter 12"],
            [p.title for p in pages])

    def test_update_latest_revisions(self):
        update_latest_revisions(self.wiki, (WikiPage(id=i) for i in [2581, 14424, 2752]), self.session)

        self.assertEqual(3, self.session.query(WikiPage).count())

        for page in self.session.query(WikiPage).all():
            self.assertLess(1, page.latest_revision_online)
            self.assertIsNone(page.revision_id)

    def test_update_pages(self):
        self.fail()

    def test_update_outdated_pages(self):
        self.fail()
