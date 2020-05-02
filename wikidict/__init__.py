import os
from urllib.parse import urlparse

import sqlalchemy.engine.url
from mediawiki import mediawiki
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from wikidict.model import Base

__version__ = '0.0.0'
__user_agent__ = 'wikidict/{} (https://github.com/mkouhia/wikidict; mkouhia@iki.fi) ' \
                 'pymediawiki/{}'.format(__version__, mediawiki.VERSION)

Session = sessionmaker()


def get_session(api_url) -> Session:
    db_path = '{}.db'.format(urlparse(api_url).netloc)
    engine = create_engine(sqlalchemy.engine.url.URL(drivername='sqlite', database=db_path))
    Session.configure(bind=engine)
    return Session()


@event.listens_for(Engine, 'connect')
def set_sqlite_pragma(dbapi_connection, connection_record):
    """Set pragma foreign_keys=ON for SQLite database"""
    cursor = dbapi_connection.cursor()
    cursor.execute('PRAGMA foreign_keys=ON')
    cursor.close()


def ensure_database(session: Session):
    """Make sure that SQLite database and tables exist"""
    engine = session.get_bind()
    if not sqlite_file_exists(engine.url.database):
        Base.metadata.create_all(engine)


def delete_database(session: Session):
    engine = session.get_bind()
    if sqlite_file_exists(engine.url.database):
        os.remove(engine.url.database)


def sqlite_file_exists(db_file_name):
    """Check if SQLite file exists

    See sqlalchemy_utils.functions.database"""
    if not os.path.isfile(db_file_name) or os.path.getsize(db_file_name) < 100:
        return False

    with open(db_file_name, 'rb') as f:
        header = f.read(100)

    return header[:16] == b'SQLite format 3\x00'
