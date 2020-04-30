import collections
import logging
import re
from itertools import zip_longest
from typing import Iterator, List, Iterable, Tuple, Dict, Any, Callable

from mediawiki import MediaWiki, MediaWikiException
from sqlalchemy import or_
from sqlalchemy.orm import Session

from wikidict.model import WikiPage, Category, get_or_create

logger = logging.getLogger(__name__)


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

    def update_latest_revisions(self, session: Session, page_ids: Iterable[int] = None, page_titles: Iterable[str] = None):
        """Get latest revision ID for all pages, update database

        :param session: sql database session
        :param page_ids: page IDs. If None, use page_titles.
        :param page_titles: page titles, alternative to page IDs.
        """
        max_pages = 50
        query_params = {
            'prop': 'revisions',
            'rvprop': 'ids',
        }

        based_on = 'pageids' if page_ids is not None else 'titles'

        # Take page batches from input iterator
        for group in self._iterable_grouper(page_ids or page_titles, n=max_pages):
            query_params[based_on] = '|'.join(str(s) for s in group if s is not None)
            logger.info('Update latest revisions: {}={}'.format(based_on, query_params[based_on]))

            for (page_id, revision_id) in self._continued_response(query_params, self._parse_revision):
                session.merge(WikiPage(id=page_id, latest_revision_online=revision_id))

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
        self.update_pages(session, page_ids = (p.id for p in pages))

    def update_pages(self, session: Session, page_ids: Iterable[int] = None, page_titles: Iterable[str] = None) -> None:
        """Download pages, save to database.

        :param session: sql database session
        :param page_ids: page IDs. If None, use page_titles.
        :param page_titles: page titles, alternative to page IDs.
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

        based_on = 'pageids' if page_ids is not None else 'titles'

        for group in self._iterable_grouper(page_ids or page_titles, n=max_pages):
            query_params[based_on] = '|'.join(str(s) for s in group if s is not None)
            logger.info('Update pages: {}={}'.format(based_on, query_params[based_on]))

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
            title = obj['title']
            logger.debug("Parsing wiki page for '{}'".format(title))

            content = obj['revisions'][0]['*']

            m = re.match('#REDIRECT \\[\\[([^\\]]+)\\]\\].*', content)
            redirect_to = None if m is None else session.query(WikiPage).filter(
                WikiPage.title == m.group(1)).first()

            page = session.query(WikiPage).get(obj['pageid']) or WikiPage(id=obj['pageid'])

            page.revision_id = obj['revisions'][0]['revid']
            page.latest_revision_online = obj['revisions'][0]['revid']
            page.content = content
            page.title = title
            page.redirect_to = redirect_to

            logger.debug("Page: " + str(page))

            if 'categories' in obj:
                for category_obj in obj['categories']:
                    cat = get_or_create(session, Category, name=re.sub('^Category:', '', category_obj['title']))
                    page.categories.append(cat)

            session.merge(page)


        return ret

    @staticmethod
    def _iterable_grouper(iterable, n: int):
        args = [iter(iterable)] * n
        return zip_longest(*args)
