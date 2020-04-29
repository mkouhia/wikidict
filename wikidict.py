import contextlib
import sys

from mediawiki import MediaWiki, MediaWikiPage
from sqlalchemy import create_engine, event, or_
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from wikidict.dictentry import DictEntry
from wikidict.model import WikiPage, Base
from wikidict.wiki import get_pages, update_latest_revisions, update_outdated_pages

engine = create_engine('sqlite:///wikidict.db', echo=True)


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """Set pragma foreign_keys=ON for SQLite database"""
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def recreate_tables():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def save_dictionary(session: Session, file_path: str = None):
    """Save wiki dictionary as text file
    :param session:
    :param file_path: file path, or '-'/None for stdout
    """
    with _smart_open(file_path) as f:
        page: WikiPage
        for page in session.query(WikiPage).order_by('title').all():
            f.write(page.to_dict_entry())


@contextlib.contextmanager
def _smart_open(filename=None):
    if filename and filename != '-':
        fh = open(filename, 'w')
    else:
        fh = sys.stdout

    try:
        yield fh
    finally:
        if fh is not sys.stdout:
            fh.close()


def main():

    url = 'http://awoiaf.westeros.org/api.php'
    wiki = MediaWiki(url)

    recreate_tables()

    Session = sessionmaker(bind=engine)
    session = Session()

    pages = get_pages(wiki, query_from="Azor Ahai", max_pages=15)
    update_latest_revisions(wiki, pages, session)

    update_outdated_pages(wiki, session)

    # for page in session.query(WikiPage).order_by('title').all():
    #     print(page)

    save_dictionary(session, "-")


if __name__ == '__main__':
    main()
