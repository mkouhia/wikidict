from unittest import TestCase

from tests import session, engine
from wikidict.model import WikiPage, Category, Base, get_or_create


class TestWikiPage(TestCase):

    def setUp(self) -> None:
        session.expunge_all()
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

    def test_str(self):
        cat1 = Category(name='cat1')
        cat2 = Category(name='cat2')
        page_from = WikiPage(title='Short title')
        page = WikiPage(id=1, title='Long title', revision_id='12', latest_revision_online='111',
                        redirect_to_id=None, categories=[cat1, cat2],
                        content='This sentence must definitely be longer than 50 characters.')
        page_from.redirect_to = page
        session.add(page, page_from)

        self.assertEqual(
            "(id=1, title='Long title', revision_id=12, latest_revision_online=111, redirect_from=['Short title'], "
            "redirect_to_id=None, categories=['cat1', 'cat2'], "
            "content='This sentence must definitely be longer than 50 ch[...]')",
            str(page)
        )

    def test_get_or_create(self):
        cat1 = Category(name='cat1')
        session.add(cat1)
        session.commit()

        cat1a = get_or_create(session, Category, name='cat1')
        self.assertEqual(cat1, cat1a)

        cat2 = get_or_create(session, Category, name='cat2')
        session.commit()

        self.assertEqual(cat2, session.query(Category).filter_by(name='cat2').first())
