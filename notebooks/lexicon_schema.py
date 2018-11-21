# coding: utf-8
# Created using sqlacodegen
from sqlalchemy import Column, DateTime, Index, String, Text
from sqlalchemy.dialects.mysql import BIGINT, BIT, TINYINT, VARCHAR
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class AnalyzedWordform(Base):
    __tablename__ = 'analyzed_wordforms'
    __table_args__ = (
        Index('awfKey', 'part_of_speech', 'lemma_id', 'wordform_id',
              'multiple_lemmata_analysis_id', 'derivation_id', unique=True),
    )

    analyzed_wordform_id = Column(BIGINT(20), primary_key=True)
    part_of_speech = Column(String(255), nullable=False)
    lemma_id = Column(BIGINT(20), nullable=False, index=True)
    wordform_id = Column(BIGINT(20), nullable=False, index=True)
    multiple_lemmata_analysis_id = Column(BIGINT(20), nullable=False)
    derivation_id = Column(BIGINT(20), nullable=False, index=True)
    verified_by = Column(BIGINT(20))
    verification_date = Column(DateTime)

    def __init__(self, pos_tag, lemma_id, wordform_id):
        self.part_of_speech = pos_tag
        self.lemma_id = lemma_id
        self.wordform_id = wordform_id
        self.multiple_lemmata_analysis_id = 0
        self.derivation_id = 0


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

    def __init__(self, doc_id):
        self.persistent_id = doc_id


class Lemmata(Base):
    __tablename__ = 'lemmata'
    __table_args__ = (
        Index('lemmaKeyTuple', 'modern_lemma', 'gloss', 'lemma_part_of_speech',
              'ne_label', 'language_id', unique=True),
    )

    lemma_id = Column(BIGINT(20), primary_key=True, index=True)
    modern_lemma = Column(VARCHAR(255))
    gloss = Column(String(255))
    persistent_id = Column(String(255), index=True)
    lemma_part_of_speech = Column(String(255), index=True)
    ne_label = Column(String(255))
    portmanteau_lemma_id = Column(BIGINT(20), index=True)
    language_id = Column(TINYINT(3))

    def __init__(self, lemma, pos_tag):
        self.modern_lemma = lemma
        self.lemma_part_of_speech = pos_tag


class TokenAttestation(Base):
    __tablename__ = 'token_attestations'
    __table_args__ = (
        Index('tlaKey', 'analyzed_wordform_id', 'derivation_id', 'document_id',
              'start_pos', 'end_pos', unique=True),
    )

    attestation_id = Column(BIGINT(20), primary_key=True)
    token_id = Column(BIGINT(20))
    quote = Column(Text)
    analyzed_wordform_id = Column(BIGINT(20), nullable=False)
    derivation_id = Column(BIGINT(20), nullable=False)
    document_id = Column(BIGINT(20), nullable=False)
    start_pos = Column(BIGINT(20), nullable=False)
    end_pos = Column(BIGINT(20), nullable=False)

    def __init__(self, awf_id, doc_id):
        self.analyzed_wordform_id = awf_id
        self.document_id = doc_id

        self.derivation_id = 0
        self.start_pos = 0
        self.end_pos = 0


class Wordform(Base):
    __tablename__ = 'wordforms'

    wordform_id = Column(BIGINT(20), primary_key=True)
    wordform = Column(VARCHAR(255), unique=True)
    has_analysis = Column(BIT(1))
    wordform_lowercase = Column(VARCHAR(255), nullable=False, index=True)

    def __init__(self, wordform):
        self.wordform = wordform
        self.has_analysis = False
        self.wordform_lowercase = wordform.lower()
