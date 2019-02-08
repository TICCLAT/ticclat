# coding: utf-8
from sqlalchemy import Column, String, Table, ForeignKey, Unicode, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import BIGINT, BIT
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


class TextAttestation(Base):
    __tablename__ = 'text_attestations'

    attestation_id = Column(BIGINT(20), primary_key=True)
    frequency = Column(BIGINT(20))
    wordform_id = Column(BIGINT(20), ForeignKey('wordforms.wordform_id'))
    document_id = Column(BIGINT(20), ForeignKey('documents.document_id'))

    ta_document = relationship('Document', back_populates='document_wordforms')
    ta_wordform = relationship('Wordform', back_populates='wordform_documents')


class WordformLink(Base):
   __tablename__ = 'wordform_links'

   wordform_link_id = Column(BIGINT(20), primary_key=True)
   wordform_1_id = Column(BIGINT(20), ForeignKey('wordforms.wordform_id'))
   wordform_2_id = Column(BIGINT(20), ForeignKey('wordforms.wordform_id'))
   lexicon_id = Column(BIGINT(20), ForeignKey('lexica.lexicon_id'))

   wordform_1 = relationship('Wordform', back_populates="wordforms")
   wordform_2 = relationship('Wordform', back_populates="wordforms")
   lexicon = relationship('Lexicon', back_populates="lexica")


# wordform_link = Table('wordform_links', Base.metadata,
#     Column('wordform_link_id', BIGINT(20), primary_key=True),
#     Column('wordform_1_id', BIGINT(20), ForeignKey('wordforms.wordform_id')),
#     Column('wordform_2_id', BIGINT(20), ForeignKey('wordforms.wordform_id'))
#     )


# source_x_wordform_link = Table('source_x_wordform_link', Base.metadata,
#                                Column('source_x_wordform_link_id', BIGINT(20), primary_key=True),
#                                Column('wordform_link_id', BIGINT(20), ForeignKey('wordform_links.wordform_link_id')),
#                                Column('lexicon_id', BIGINT(20), ForeignKey('lexica.lexicon_id'))
#                                )


class Corpus(Base):
    __tablename__ = 'corpora'

    corpus_id = Column(BIGINT(20), primary_key=True)
    name = Column(String(255))
    corpus_documents = relationship('Document',
                                    secondary=corpusId_x_documentId,
                                    back_populates='document_corpora')


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

    document_corpora = relationship('Corpus', secondary=corpusId_x_documentId,
                                    back_populates='corpus_documents')
    document_wordforms = relationship('TextAttestation', back_populates='ta_document')


lexical_source_wordform = Table('lexical_source_wordform', Base.metadata,
                                Column('wordform_source_id', BIGINT(20), primary_key=True),
                                Column('lexicon_id', BIGINT(20), ForeignKey('lexica.lexicon_id')),
                                Column('wordform_id', BIGINT(20), ForeignKey('wordforms.wordform_id'))
                                )


class Lexicon(Base):
    __tablename__ = 'lexica'

    lexicon_id = Column(BIGINT(20), primary_key=True)
    lexicon_name = Column(String(255))
    vocabulary = Column(Boolean)

    lexicon_wordforms = relationship('Wordform',
                                     secondary=lexical_source_wordform,
                                     back_populates='wf_lexica')
    wordform_links = relationship("WordformLink")

    def __str__(self):
        return '<Lexicon {}>'.format(self.lexicon_name)


class Anahash(Base):
    __tablename__ = 'anahashes'

    anahash_id = Column(BIGINT(20), primary_key=True)
    anahash = Column(BIGINT(20), unique=True, index=True)

    def __str__(self):
        return '<Anahash {}>'.format(self.anahash)


class Wordform(Base):
    __tablename__ = 'wordforms'

    wordform_id = Column(BIGINT(20), primary_key=True)
    wordform = Column(Unicode(255, convert_unicode=False), unique=True, index=True)
    anahash_id = Column(BIGINT(20), ForeignKey('anahashes.anahash_id'))

    anahash = relationship('Anahash')
    wordform_lowercase = Column(Unicode(255, convert_unicode=False), nullable=False, index=True)

    wf_lexica = relationship('Lexicon', secondary=lexical_source_wordform,
                             back_populates='lexicon_wordforms')
    wordform_documents = relationship('TextAttestation', back_populates='ta_wordform')
    # links = relationship('Wordform', secondary=wordform_link,
    #                      primaryjoin=wordform_link.c.wordform_1_id == wordform_id,
    #                      secondaryjoin=wordform_link.c.wordform_2_id == wordform_id
    #                      )
    links = relationship("WordformLink", back_populates="wordform")

    def __str__(self):
        return '<Wordform {}>'.format(self.wordform_lowercase)
