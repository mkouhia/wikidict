import argparse
import logging

from mediawiki import MediaWiki

from wikidict import Session, delete_database, ensure_database
from wikidict.dictionary import Dictionary
from wikidict.wiki import WikiDownloader

logging.basicConfig(level=logging.WARNING)


def main():
    parser = argparse.ArgumentParser(description='Create a dictionary from wiki articles')

    parser.add_argument('-o', '--output', help='output file path', default='-')

    parser.add_argument('--rebuild', help='Discard existing database and start with fresh one',
                        default=False, action="store_true")

    args = parser.parse_args()

    if args.rebuild:
        delete_database()
    ensure_database()

    print("API url: {}".format(args.api_url))
    wiki = MediaWiki('https://awoiaf.westeros.org/index.php')
    wiki_downloader = WikiDownloader(wiki)

    session = Session()

    pages = wiki_downloader.get_pages(query_from='', max_pages=None)
    wiki_downloader.update_latest_revisions(session, page_ids=(p.id for p in pages))
    # wiki_downloader.update_outdated_pages(session)

    dictionary = Dictionary(session)
    dictionary.save('-')


if __name__ == '__main__':
    main()
