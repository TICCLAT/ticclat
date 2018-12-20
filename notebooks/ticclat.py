# coding: utf-8
from sqlalchemy import Column, String, Table, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import BIGINT, BIT, VARCHAR
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


corpusId_x_documentId = Table('corpusId_x_documentId', Base.metadata,
        Column('corpus_id', BIGINT(20), ForeignKey('corpora.corpus_id')),
        Column('document_id', BIGINT(20), ForeignKey('documents.document_id'))
    )


# Removed:
# __table_args__ = (
#     Index('tlaKey', 'wordform_id', 'document_id', unique=True),
# )
text_attestation = Table('text_attestations', Base.metadata,
        Column('attestation_id', BIGINT(20), primary_key=True),
        Column('frequency', BIGINT(20)),
        Column('wordform_id', BIGINT(20), ForeignKey('wordforms.wordform_id')),
        Column('document_id', BIGINT(20), ForeignKey('documents.document_id'))
    )

wordform_link = Table('wordform_links', Base.metadata,
    Column('wordform_link_id', BIGINT(20), primary_key=True),
    Column('wordform_1_id', BIGINT(20), ForeignKey('wordforms.wordform_id')),
    Column('wordform_2_id', BIGINT(20), ForeignKey('wordforms.wordform_id'))
    )


source_x_wordform_link = Table('source_x_wordform_link', Base.metadata,
    Column('source_x_wordform_link_id', BIGINT(20), primary_key=True),
    Column('wordform_link_id', BIGINT(20), ForeignKey('wordform_links.wordform_link_id')),
    Column('lexicon_id', BIGINT(20), ForeignKey('lexica.lexicon_id'))
    )


class Corpus(Base):
    __tablename__ = 'corpora'

    corpus_id = Column(BIGINT(20), primary_key=True)
    name = Column(String(255))
    documents = relationship('Document', secondary=corpusId_x_documentId,
                             back_populates='corpora')


class Document(Base):
    __tablename__ = 'documents'

    document_id = Column(BIGINT(20), primary_key=True)
    persistent_id = Column(String(255), index=True)
    word_count = Column(BIGINT(20))
    encoding = Column(BIGINT(20))
    title = Column(String(255))
    year_from = Column(BIGINT(20))
    year_to = Column(BIGINT(20))
    pub_year = Column(BIGINT(20))
    author = Column(String(255))
    editor = Column(String(255))
    publisher = Column(String(255))
    publishing_location = Column(String(255))
    text_type = Column(String(255))
    region = Column(String(255))
    language = Column(String(255))
    other_languages = Column(String(255))
    spelling = Column(String(255))
    parent_document = Column(BIGINT(20), index=True)

    corpora = relationship('Corpus', secondary=corpusId_x_documentId,
                           back_populates='documents')
    wordforms = relationship('Wordform', secondary=text_attestation,
                             back_populates='documents')


lexical_source_wordform = Table('lexical_source_wordform', Base.metadata,
        Column('wordform_source_id', BIGINT(20), primary_key=True),
        Column('lexicon_id', BIGINT(20), ForeignKey('lexica.lexicon_id')),
        Column('wordform_id', BIGINT(20), ForeignKey('wordforms.wordform_id'))
    )


class Lexicon(Base):
    __tablename__ = 'lexica'

    lexicon_id = Column(BIGINT(20), primary_key=True)
    lexicon_name = Column(String(255))

    wordforms = relationship('Wordform', secondary=lexical_source_wordform,
                             back_populates='lexica')

    def __str__(self):
        return '<Lexicon {}>'.format(self.lexicon_name)


class Wordform(Base):
    __tablename__ = 'wordforms'

    wordform_id = Column(BIGINT(20), primary_key=True)
    wordform = Column(VARCHAR(255), unique=True)
    anahash_id = Column(BIGINT(20), ForeignKey('anahashes.anahash_id'))
    anahash = relationship('Anahash')
    has_analysis = Column(BIT(1))
    wordform_lowercase = Column(VARCHAR(255), nullable=False, index=True)

    lexica = relationship('Lexicon', secondary=lexical_source_wordform,
                          back_populates='wordforms')
    documents = relationship('Document', secondary=text_attestation,
                             back_populates='wordforms')
    links = relationship('Wordform', secondary=wordform_link,
                         primaryjoin=wordform_link.c.wordform_1_id == wordform_id,
                         secondaryjoin=wordform_link.c.wordform_2_id == wordform_id
                         )

    def __str__(self):
        return '<Worfdform {}>'.format(self.wordform_lowercase)


class Anahash(Base):
    __tablename__ = 'anahashes'

    anahash_id = Column(BIGINT(20), primary_key=True)
    anahash = Column(BIGINT(20))
