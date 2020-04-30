from typing import Any

from sqlalchemy import Column, Integer, String, Text, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, Session


Base = declarative_base()

category_association = Table('category_association', Base.metadata,
                             Column('page_id', Integer, ForeignKey('pages.id')),
                             Column('category_id', Integer, ForeignKey('categories.id'))
                             )


class WikiPage(Base):
    __tablename__ = 'pages'
    id = Column(Integer, primary_key=True)
    revision_id = Column(Integer)
    latest_revision_online = Column(Integer)
    content = Column(Text)
    title = Column(String(64))
    redirect_to_id = Column(Integer, ForeignKey('pages.id'))
    redirect_from = relationship('WikiPage', backref=backref('redirect_to', remote_side=[id]))
    categories = relationship('Category', secondary=category_association, backref='pages')

    def __str__(self) -> str:
        return '(id={}, title={}, revision_id={}, latest_revision_online={}, redirect_from={}, ' \
               'redirect_to_id={}, categories={}, content={})' \
            .format(
                self.id, self.title, self.revision_id, self.latest_revision_online, self.redirect_from,
                self.redirect_to_id, self.categories, self.content)


class Category(Base):
    __tablename__ = 'categories'
    id = Column(Integer, primary_key=True)
    name = Column(String(64))


def get_or_create(session: Session, model: Base, **kwargs: Any) -> Any:
    """Get or create database object from session
    :param session: sql database session
    :param model: model class
    :param kwargs: arguments for session.query(model).filter_by(**kwargs)
    :return: first query result or None
    """
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        session.add(instance)
        session.commit()
        return instance
