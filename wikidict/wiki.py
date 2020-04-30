import collections
import re
from itertools import zip_longest
from typing import Iterator, List, Iterable, Tuple, Dict, Any, Callable

from mediawiki import MediaWiki, MediaWikiException
from sqlalchemy import or_
from sqlalchemy.orm import Session

from wikidict.model import WikiPage, Category, get_or_create


class WikiDownloader(object):

    def __init__(self, wiki: MediaWiki):
        self.wiki = wiki

    def get_pages(self, query_from='', max_pages=100000) -> Iterator[WikiPage]:
        """Get pages (id & title)
        :param query_from: query: get page names starting from this (empty string: from the beginning)
        :param max_pages: retrieve approximately this amount of pages at maximum
        """
        n_batch = 500 if max_pages > 500 else max_pages
        query_params = {'list': 'allpages', 'aplimit': n_batch, 'apfrom': query_from}

        yield from self._continued_response(query_params, self._parse_all_pages, max_pages)

    @staticmethod
    def _parse_all_pages(response) -> List[WikiPage]:
        return [WikiPage(id=page['pageid'], title=page['title']) for page in response['query']['allpages']]

    def update_latest_revisions(self, pages: Iterator[WikiPage], session: Session):
        """Get latest revision ID for all pages, update database
        :param pages: update content of these pages to the database
        :param session: sql database session
        """
        max_pages = 50
        query_params = {
            'prop': 'revisions',
            'rvprop': 'ids',
        }

        # Take page batches from input iterator
        for group in self._iterable_grouper(pages, n=max_pages):
            page_dict = {page.id: page for page in group if page is not None}
            query_params['pageids'] = '|'.join(str(i) for i in page_dict.keys())

            for (page_id, revision_id) in self._continued_response(query_params, self._parse_revision):
                page = page_dict.get(page_id)
                page.latest_revision_online = revision_id
                session.merge(page)

            session.commit()

    @staticmethod
    def _parse_revision(response) -> List[Tuple[int, int]]:
        """Parse revision ids from response
        :param response: parsed dict from requests.Session.get().json()
        :return: List of tuples (page id -> revision id)
        """
        response_pages = response['query']['pages']
        return [(response_pages[i]['pageid'], response_pages[i]['revisions'][0]['revid'])
                for i in response_pages]

    def _continued_response(self, query_params: Dict, result_parse_func: Callable[[Dict], Any], max_results=100000)\
            -> Iterator[Any]:
        """Process continued response from MediaWiki API

        Follow 'continue' links until result is exhausted
        :param query_params: query parameters to wiki request
        :param result_parse_func: function that is employed to parse request JSON
        :param max_results: fetch at maximum this amount of results, until returning
        :return: iterator of result_parse_func results
        """
        received_results = 0
        while True:
            response = self.wiki.wiki_request(query_params)
            if 'error' in response:
                raise MediaWikiException(response['error']['info'])
            results = result_parse_func(response)
            for i in results:
                if received_results < max_results:
                    yield i
                    received_results += 1
                else:
                    return

            if 'continue' not in response:
                break
            else:
                query_params.update(response['continue'])

    def update_outdated_pages(self, session: Session) -> None:
        """Check database table 'pages', download outdated pages

        Download pages, whose revision_id < latest_revision_online. Save to database.
        :param session: sql database session
        """
        pages = session.query(WikiPage) \
            .filter(or_(WikiPage.revision_id == None, WikiPage.revision_id < WikiPage.latest_revision_online))  # noqa: E711
        self.update_pages(session, pages)

    def update_pages(self, session: Session, pages: Iterable[WikiPage], based_on='pageids') -> None:
        """Download pages, save to database.
        :param pages: update content of these pages to the database
        :param session: sql database session
        :param based_on: get pages based on 'pageids' or 'titles' from supplied WikiPages (supply either string)
        """
        max_pages = 50
        query_params = {
            'prop': 'revisions|categories',
            # Revisions (content)
            'rvprop': 'content|ids',
            # categories
            'cllimit': 'max',
            'clshow': '!hidden',
            # references
            'ellimit': 'max',
        }

        for group in self._iterable_grouper(pages, n=max_pages):
            if based_on == 'titles':
                refs = [page.title for page in group if page is not None]
            else:
                refs = [str(page.id) for page in group if page is not None]
            query_params[based_on] = '|'.join(refs)

            page_iterator = self._continued_response(
                query_params, lambda response: self._parse_pages_and_add(response, session))
            # Consume iterator without assigning results
            collections.deque(page_iterator, maxlen=0)

            session.commit()

    @staticmethod
    def _parse_pages_and_add(response: Dict, session: Session) -> List[WikiPage]:
        """Parse network response, add parsed pages to database
        :param response: parsed dict from requests.Session.get().json()
        :param session: sql database session
        :return: List of WikiPages, that are parsed and already merged to the session
        """
        ret = []
        for i in response['query']['pages']:
            obj = response['query']['pages'][i]

            content = obj['revisions'][0]['*']

            m = re.match('#REDIRECT \\[\\[([^\\]]+)\\]\\].*', content)
            redirect_to = None if m is None else session.query(WikiPage).filter(
                WikiPage.title == m.group(1)).first()

            page = session.query(WikiPage).get(obj['pageid']) or WikiPage(id=obj['pageid'])

            page.revision_id = obj['revisions'][0]['revid']
            page.latest_revision_online = obj['revisions'][0]['revid']
            page.content = content
            page.title = obj['title']
            page.redirect_to = redirect_to
            session.merge(page)

            if 'categories' in obj:
                for category_obj in obj['categories']:
                    cat = get_or_create(session, Category, name=re.sub('^Category:', '', category_obj['title']))
                    page.categories.append(cat)

            session.merge(page)

        return ret

    @staticmethod
    def update_redirects_from_content(session: Session):
        for page in session.query(WikiPage).filter(WikiPage.content.like('#REDIRECT%')):
            m = re.match('#REDIRECT \\[\\[([^\\]]+)\\]\\].*', page.content)
            if m is None:
                continue
            page.redirect_to = session.query(WikiPage).filter(WikiPage.title == m.group(1)).first()
            session.merge(page)

        session.commit()

    @staticmethod
    def _iterable_grouper(iterable, n: int):
        args = [iter(iterable)] * n
        return zip_longest(*args)
