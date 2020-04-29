import re
from itertools import zip_longest
from typing import Iterator, Dict, List, Iterable, Tuple

from mediawiki import MediaWiki, MediaWikiException
from sqlalchemy import or_
from sqlalchemy.orm import Session

from wikidict.model import WikiPage, Category


def get_pages(wiki: MediaWiki, query_from="", max_pages=100000) -> Iterator[WikiPage]:
    """Get pages (id & title)
    :param wiki: MediaWiki object
    :param query_from: query: get page names starting from this (empty string: from the beginning)
    :param max_pages: retrieve approximately this amount of pages at maximum
    """
    n_batch = 500 if max_pages > 500 else max_pages
    query_params = {"list": "allpages", "aplimit": n_batch, "apfrom": query_from}

    def parse_pages(response) -> List[WikiPage]:
        return [WikiPage(id=page["pageid"], title=page["title"]) for page in response["query"]["allpages"]]

    yield from _continued_response(wiki, query_params, parse_pages, max_pages)


def _continued_response(wiki, query_params, result_parse_func, max_results=100000) -> Iterator:
    received_results = 0
    while True:
        response = wiki.wiki_request(query_params)
        if "error" in response:
            raise MediaWikiException(response["error"]["info"])
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
            query_params.update(response["continue"])


def update_latest_revisions(wiki: MediaWiki, pages: Iterator[WikiPage], session: Session):
    """Get latest revision ID for all pages, update database
    :param wiki:
    :param pages:
    :param session:
    """
    max_pages = 50
    query_params = {
        "prop": "revisions",
        "rvprop": "ids",
    }

    def parse_revid(response) -> List[Tuple[int, int]]:
        response_pages = response["query"]["pages"]
        return [(response_pages[i]["pageid"], response_pages[i]["revisions"][0]["revid"])
                for i in response_pages]

    # Take page batches from input iterator
    for group in _iterable_grouper(pages, n=max_pages):
        page_dict = {page.id: page for page in group if page is not None}
        query_params['pageids'] = "|".join(str(i) for i in page_dict.keys())

        for (page_id, revision_id) in _continued_response(wiki, query_params, parse_revid):
            page = page_dict.get(page_id)
            page.latest_revision_online = revision_id
            session.merge(page)

        session.commit()


def _iterable_grouper(iterable, n: int):
    args = [iter(iterable)] * n
    return zip_longest(*args)


def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        session.add(instance)
        session.commit()
        return instance


def update_pages(wiki: MediaWiki, session: Session, pages: Iterable[WikiPage], based_on="pageids"):
    """Download pages, save to database.
    :param based_on: get pages based on 'pageids' or 'titles' from supplied wikipages (supply either string)
    """
    max_pages = 50
    query_params = {
        "prop": "revisions|categories",
        # Revisions (content)
        "rvprop": "content|ids",
        # categories
        "cllimit": "max",
        "clshow": "!hidden",
        # references
        "ellimit": "max",
    }

    def parse_and_add(response, session: Session) -> List[WikiPage]:
        ret = []
        for i in response["query"]["pages"]:
            obj = response["query"]["pages"][i]

            content = obj["revisions"][0]["*"]

            m = re.match("#REDIRECT \\[\\[([^\\]]+)\\]\\].*", content)
            redirect_to = None if m is None else session.query(WikiPage).filter(WikiPage.title == m.group(1)).first()

            page = session.query(WikiPage).get(obj["pageid"]) or WikiPage(id=obj["pageid"])

            page.revision_id = obj["revisions"][0]["revid"]
            page.latest_revision_online = obj["revisions"][0]["revid"]
            page.content = content
            page.title = obj["title"]
            page.redirect_to=redirect_to
            session.merge(page)

            if 'categories' in obj:
                for category_obj in obj['categories']:
                    cat = get_or_create(session, Category, name=re.sub('^Category:', '', category_obj["title"]))
                    page.categories.append(cat)

            session.merge(page)

        return ret

    for group in _iterable_grouper(pages, n=max_pages):
        if based_on == "titles":
            refs = [page.title for page in group if page is not None]
        else:
            refs = [str(page.id) for page in group if page is not None]
        query_params[based_on] = "|".join(refs)

        for page_new in _continued_response(wiki, query_params, lambda response: parse_and_add(response, session)):
            pass

        session.commit()

def update_redirects_from_content(session: Session):
    for page in session.query(WikiPage).filter(WikiPage.content.like("#REDIRECT%")):
        m = re.match("#REDIRECT \\[\\[([^\\]]+)\\]\\].*", page.content)
        if m is None:
            continue
        page.redirect_to = session.query(WikiPage).filter(WikiPage.title == m.group(1)).first()
        session.merge(page)

    session.commit()

def update_outdated_pages(wiki: MediaWiki, session: Session):
    """Check database table 'pages', download outdated pages

    Download pages, whose revision_id < latest_revision_online. Save to database.
    :param wiki:
    :param session:
    """
    pages = session.query(WikiPage) \
        .filter(or_(WikiPage.revision_id == None, WikiPage.revision_id < WikiPage.latest_revision_online))
    update_pages(wiki, session, pages)
