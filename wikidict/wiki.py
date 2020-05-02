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

    def get_page_list(self, session: Session, query_from='', max_pages: int = None) -> None:
        """Get pages (id & title), commit to database

        :param session: sql database session
        :param query_from: query: get page names starting from this (empty string: from the beginning)
        :param max_pages: fetch at maximum this amount of results, until returning; None: get until exhaustion
        """
        logger.info("Get page list")

        n_batch = 500 if (max_pages is None or max_pages > 500) else max_pages
        query_params = {'list': 'allpages', 'aplimit': n_batch, 'apfrom': query_from}

        page_iterator = self._continued_response(
            query_params, lambda response: self._merge_page_list(response=response, session=session), max_pages)

        list(page_iterator)


    @staticmethod
    def _merge_page_list(response: Dict, session: Session) -> List[int]:
        ret = []
        for page_dict in response['query']['allpages']:
            ret.append(page_dict['pageid'])
            page = WikiPage(id=page_dict['pageid'], title=page_dict['title'])
            session.merge(page)
        session.commit()

        return ret

    def update_latest_revisions(self, session: Session, page_ids: Iterable[int] = None,
                                page_titles: Iterable[str] = None) -> None:
        """Get latest revision ID for all pages, update database

        :param session: sql database session
        :param page_ids: page IDs. If None, use page_titles.
        :param page_titles: page titles, alternative to page IDs. If both lists are None, get list from database.
        """
        logger.info('Update latest revisions')

        max_pages = 50
        query_params = {
            'prop': 'revisions',
            'rvprop': 'ids',
        }

        if page_ids is None and page_titles is None:
            based_on = 'pageids'
            page_ids = (page.id for page in session.query(WikiPage))
        else:
            based_on = 'pageids' if page_ids is not None else 'titles'

        # Take page batches from input iterator
        for group in self._iterable_grouper(page_ids or page_titles, n=max_pages):
            query_params[based_on] = '|'.join(str(s) for s in group if s is not None)
            logger.debug('Update latest revisions: {}={}'.format(based_on, query_params[based_on]))

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

    def _continued_response(self, query_params: Dict, result_parse_func: Callable[[Dict], Any],
                            max_results: int = None) -> Iterator[Any]:
        """Process continued response from MediaWiki API

        Follow 'continue' links until result is exhausted
        :param query_params: query parameters to wiki request
        :param result_parse_func: function that is employed to parse request JSON
        :param max_results: fetch at maximum this amount of results, until returning; None: get until exhaustion
        :return: iterator of result_parse_func results
        """
        received_results = 0
        while True:
            response = self.wiki.wiki_request(query_params)
            if 'error' in response:
                raise MediaWikiException(response['error']['info'])
            if 'warnings' in response:
                logger.warning('API warning' + response['warnings']['main']['*'])
            results = result_parse_func(response)
            for i in results:
                if max_results is None or received_results < max_results:
                    yield i
                    received_results += 1
                else:
                    return

            if 'continue' not in response:
                break
            else:
                query_params.update(response['continue'])

    def update_outdated_pages(self, session: Session) -> List[int]:
        """Check database table 'pages', download outdated pages

        Download pages, whose revision_id < latest_revision_online. Save to database.

        :param session: sql database session
        :return: downloaded page IDs
        """
        pages = session.query(WikiPage) \
            .filter(or_(WikiPage.revision_id == None,  # noqa: E711
                        WikiPage.revision_id < WikiPage.latest_revision_online))
        return self.update_pages(session, page_ids=(p.id for p in pages), follow_redirects=False)

    def update_pages(self, session: Session, page_ids: Iterable[int] = None, page_titles: Iterable[str] = None,
                     follow_redirects=True):
        """Download pages, save to database.

        :param session: sql database session
        :param page_ids: page IDs. If None, use page_titles.
        :param page_titles: page titles, alternative to page IDs.
        :param follow_redirects: re-download redirected pages and update those in database
        :return: downloaded page IDs
        """
        max_pages = 50
        query_params = {
            'prop': 'revisions|categories',
            # Revisions (content)
            'rvprop': 'content|ids',
            # categories
            'cllimit': 'max',
            'clshow': '!hidden',
        }

        based_on = 'pageids' if page_ids is not None else 'titles'

        downloaded_ids: List[int] = []
        redirects: List[Tuple[int, str]] = []

        for group in self._iterable_grouper(page_ids or page_titles, n=max_pages):
            query_params[based_on] = '|'.join(str(s) for s in group if s is not None)
            logger.info('Update pages: {}={}'.format(based_on, query_params[based_on]))

            _downloaded, _redirects = self._continued_response(
                query_params, lambda response: self._parse_pages_and_add(response, session))

            downloaded_ids.extend(_downloaded)
            redirects.extend(_redirects)

            session.commit()

        # Download redirected pages, set connections
        if follow_redirects and len(redirects) > 0:
            redirect_source_ids, redirect_target_titles = zip(*redirects)
            _downloaded = self.update_pages(session, page_titles=redirect_target_titles,
                                            follow_redirects=follow_redirects)
            self.link_redirects(session, redirect_source_ids)

            downloaded_ids.extend(_downloaded)

        return downloaded_ids

    @staticmethod
    def _parse_pages_and_add(response: Dict, session: Session) -> Tuple[List[int], List[Tuple[int, str]]]:
        """Parse network response, add parsed pages to database

        :param response: parsed dict from requests.Session.get().json()
        :param session: sql database session
        :return: list of downloaded page IDs,
                 list of (page ID, target title) tuples for redirects, that need to be resolved
        """
        downloaded_ids = []
        pending_redirects = []
        for i in response['query']['pages']:
            obj = response['query']['pages'][i]

            if 'missing' in obj:
                logger.warning('Missing object in API request result: ' + obj)
                continue

            title = obj['title']
            page_id = obj['pageid']

            logger.debug("Parsing wiki page for '{}'".format(title))

            content = obj['revisions'][0]['*']

            downloaded_ids.append(page_id)

            m = re.match('#REDIRECT \\[\\[([^\\]]+)\\]\\].*', content)
            if m is not None:
                pending_redirects.append((page_id, m.group(1)))

            page = session.query(WikiPage).get(page_id) or WikiPage(id=page_id)

            page.revision_id = obj['revisions'][0]['revid']
            page.latest_revision_online = obj['revisions'][0]['revid']
            page.content = content
            page.title = title

            logger.debug("Page: " + str(page))

            if 'categories' in obj:
                for category_obj in obj['categories']:
                    cat = get_or_create(session, Category, name=re.sub('^Category:', '', category_obj['title']))
                    page.categories.append(cat)

            session.merge(page)

        return downloaded_ids, pending_redirects

    @staticmethod
    def link_redirects(session: Session, page_ids: List[int]):
        """Link redirection pages and commit

        :param session: sql database session
        :param page_ids: page IDs, from which to resolve the redirects
        """
        for source_id in page_ids:
            source_page: WikiPage = session.query(WikiPage).get(source_id)
            m = re.match('#REDIRECT \\[\\[([^\\]]+)\\]\\].*', source_page.content)

            if m is None:
                continue

            target_title = m.group(1)
            target_page = session.query(WikiPage).filter(WikiPage.title == target_title).first()

            if target_page is None:
                continue

            source_page.redirect_to = target_page
            session.add(source_page, target_page)

        session.commit()

    @staticmethod
    def _iterable_grouper(iterable, n: int):
        args = [iter(iterable)] * n
        return zip_longest(*args)
