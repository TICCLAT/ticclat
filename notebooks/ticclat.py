# coding: utf-8
from sqlalchemy import Column, Index, String
from sqlalchemy.dialects.mysql import BIGINT, BIT, VARCHAR
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Corpus(Base):
    __tablename__ = 'corpora'

    corpus_id = Column(BIGINT(20), primary_key=True)
    name = Column(String(255))


class CorpusIdXDocumentId(Base):
    __tablename__ = 'corpusId_x_documentId'

    corpus_id = Column(BIGINT(20), primary_key=True, nullable=False)
    document_id = Column(BIGINT(20), primary_key=True, nullable=False)


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


class Lexica(Base):
    __tablename__ = 'lexica'

    lexicon_id = Column(BIGINT(20), primary_key=True)
    lexicon_name = Column(String(255))


class LexicalSourceWordform(Base):
    __tablename__ = 'lexical_source_wordform'

    wordform_source_id = Column(BIGINT(20), primary_key=True)
    foreign_id = Column(String(255))
    label = Column(String(255))
    wordform_id = Column(BIGINT(20), index=True)
    lexicon_id = Column(BIGINT(20), index=True)


class TextAttestation(Base):
    __tablename__ = 'text_attestations'
    __table_args__ = (
        Index('tlaKey', 'analyzed_wordform_id', 'document_id', unique=True),
    )

    attestation_id = Column(BIGINT(20), primary_key=True)
    frequency = Column(BIGINT(20))
    analyzed_wordform_id = Column(BIGINT(20), nullable=False)
    document_id = Column(BIGINT(20), nullable=False)


class Wordform(Base):
    __tablename__ = 'wordforms'

    wordform_id = Column(BIGINT(20), primary_key=True)
    wordform = Column(VARCHAR(255), unique=True)
    has_analysis = Column(BIT(1))
    wordform_lowercase = Column(VARCHAR(255), nullable=False, index=True)
