import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite://')
session = sessionmaker(bind=engine)()

logging.basicConfig(level=logging.WARNING)
