import argparse
import logging

from mediawiki import MediaWiki

from wikidict import Session, delete_database, ensure_database, __version__, __user_agent__
from wikidict.dictionary import Dictionary
from wikidict.wiki import WikiDownloader

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Create a dictionary from wiki articles')

    parser.add_argument('-o', '--output', help='output file path', default='-')

    parser.add_argument('-u', '--api-url', help='API url for query',
                        default='https://en.wikipedia.org/w/api.php')
    parser.add_argument('-a', '--user-agent', help='User agent', default=__user_agent__)

    parser.add_argument('-v', '--version', help='Display version and exit')
    parser.add_argument('--rebuild', help='Discard existing database and start with fresh one',
                        default=False, action="store_true")

    args = parser.parse_args()

    logger.info(args)

    if args.version:
        print(__version__)
    if args.rebuild:
        delete_database()
    ensure_database()

    wiki = MediaWiki(url=args.api_url, user_agent=args.user_agent)
    wiki_downloader = WikiDownloader(wiki)

    session = Session()

    pages = wiki_downloader.get_pages(query_from='', max_pages=None)
    wiki_downloader.update_latest_revisions(session, page_ids=(p.id for p in pages))
    # wiki_downloader.update_outdated_pages(session)

    dictionary = Dictionary(session)
    dictionary.save('-')


if __name__ == '__main__':
    main()
