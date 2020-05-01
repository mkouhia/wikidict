import os

from mediawiki import mediawiki
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from wikidict.model import Base

__version__ = '0.0.0'
__user_agent__ = 'wikidict/{} (https://github.com/mkouhia/wikidict; mkouhia@iki.fi) ' \
                 'pymediawiki/{}'.format(__version__, mediawiki.VERSION)

db_file_name = 'wikidict.db'
engine = create_engine('sqlite:///' + db_file_name)
Session = sessionmaker(bind=engine)


@event.listens_for(Engine, 'connect')
def set_sqlite_pragma(dbapi_connection, connection_record):
    """Set pragma foreign_keys=ON for SQLite database"""
    cursor = dbapi_connection.cursor()
    cursor.execute('PRAGMA foreign_keys=ON')
    cursor.close()


def ensure_database():
    """Make sure that SQLite database and tables exist"""
    if not sqlite_file_exists():
        Base.metadata.create_all(engine)


def delete_database():
    if sqlite_file_exists():
        os.remove(db_file_name)


def sqlite_file_exists():
    """Check if SQLite file exists

    See sqlalchemy_utils.functions.database"""
    if not os.path.isfile(db_file_name) or os.path.getsize(db_file_name) < 100:
        return False

    with open(db_file_name, 'rb') as f:
        header = f.read(100)

    return header[:16] == b'SQLite format 3\x00'
