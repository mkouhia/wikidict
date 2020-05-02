import argparse
import logging

from mediawiki import MediaWiki

from wikidict import delete_database, ensure_database, __version__, __user_agent__, get_session
from wikidict.dictionary import Dictionary
from wikidict.wiki import WikiDownloader

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Create a dictionary from wiki articles')

    parser.add_argument('-o', '--output', help='output file path (default: stdout)', default='-')
    parser.add_argument('--log-level', help='log level: DEBUG/INFO/WARNING/ERROR (default: WARNING)',
                        default='WARNING')

    parser.add_argument('-a', '--api-url', help='API url for query (default: https://awoiaf.westeros.org/api.php)',
                        default='https://awoiaf.westeros.org/api.php')
    parser.add_argument('-x', '--user-agent', help='Custom user agent', default=__user_agent__)

    parser.add_argument('-d', '--download', help='Download articles with outdated version',
                        action='store_true')
    parser.add_argument('-v', '--version', help='Display version and exit', action='store_true')
    parser.add_argument('--rebuild', help='Discard existing database',
                        default=False, action='store_true')

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)

    logger.info(args)

    session = get_session(args.api_url)

    if args.version:
        print(__version__)
        return

    if args.rebuild:
        delete_database(session)
    ensure_database(session)

    wiki = MediaWiki(url=args.api_url, user_agent=args.user_agent)
    wiki_downloader = WikiDownloader(wiki)

    if args.download:
        wiki_downloader.get_page_list(session)
        wiki_downloader.update_latest_revisions(session)
        wiki_downloader.update_outdated_pages(session)

    dictionary = Dictionary(session)
    dictionary.save(args.output)


if __name__ == '__main__':
    main()
