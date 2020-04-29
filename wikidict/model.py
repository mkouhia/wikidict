from mediawiki import MediaWikiPage
from sqlalchemy import Column, Integer, String, Text, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

from wikidict.dictentry import DictEntry
from wikidict.parser import Parser

Base = declarative_base()

category_association = Table('categoryassociation', Base.metadata,
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
    redirect_from = relationship("WikiPage", backref=backref('redirect_to', remote_side=[id]))
    categories = relationship("Category", secondary=category_association, backref="pages")

    @classmethod
    def create_from_page(cls, session, page: MediaWikiPage):
        obj = cls()
        session.add(obj)
        session.commit()

    def to_dict_entry(self) -> str:
        return DictEntry(
            headword=self.title,
            body=Parser(self.content)
                .remove_templates()
                .get_first_section()
                .remove_category_links()
                .to_markdown()
                .content,
            word_info=self.categories[0].name if len(self.categories) > 0 else "",
            variants=self.redirect_from).__str__()

    def __str__(self) -> str:
        return "(id={}, title={}, revision_id={}, latest_revision_online={}, redirect_from={}, redirect_to_id={}, " \
               "categories={}, content={})".format(
            self.id, self.title, self.revision_id, self.latest_revision_online, self.redirect_from, self.redirect_to_id,
            self.categories, self.content)

    def update(self, other):
        for key in ['id', 'revision_id', 'latest_revision_online', 'content', 'title', 'redirect_to_id', 'redirect_from', 'categories']:
            if getattr(other, key) is not None:
                setattr(self, key, getattr(other, key))


class Category(Base):
    __tablename__ = 'categories'
    id = Column(Integer, primary_key=True)
    name = Column(String(64))
