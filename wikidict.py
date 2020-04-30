import contextlib
import sys

from mediawiki import MediaWiki
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

from wikidict.dictionary import Dictionary
from wikidict.model import WikiPage, Base
from wikidict.wiki import WikiDownloader

engine = create_engine('sqlite:///wikidict.db')


@event.listens_for(Engine, 'connect')
def set_sqlite_pragma(dbapi_connection, connection_record):
    """Set pragma foreign_keys=ON for SQLite database"""
    cursor = dbapi_connection.cursor()
    cursor.execute('PRAGMA foreign_keys=ON')
    cursor.close()


def main():

    url = 'http://awoiaf.westeros.org/api.php'
    wiki = MediaWiki(url)
    wiki_downloader = WikiDownloader(wiki)

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    pages = wiki_downloader.get_pages(query_from='Azor Ahai', max_pages=15)
    wiki_downloader.update_latest_revisions(pages, session)

    wiki_downloader.update_outdated_pages(session)

    dictionary = Dictionary(session)
    dictionary.save('-')


if __name__ == '__main__':
    main()
