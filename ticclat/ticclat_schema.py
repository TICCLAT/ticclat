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
    #wordform_links = relationship("WordformLink")

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

    def link(self, wf):
        """Add WordformLinks between self and another wordfrom and vice versa.

        The WordformLinks are added only in the link does not yet exist.
        """
        links = [w.linked_to for w in self.links]
        if wf not in links:
            WordformLink(self, wf)
            WordformLink(wf, self)

    def __str__(self):
        return '<Wordform {}>'.format(self.wordform_lowercase)


class WordformLink(Base):
    __tablename__ = 'wordform_links'

    wordform_link_id = Column(BIGINT(20), primary_key=True)
    wordform_from = Column(BIGINT(20), ForeignKey('wordforms.wordform_id'))
    wordform_to = Column(BIGINT(20), ForeignKey('wordforms.wordform_id'))

    linked_from = relationship('Wordform', backref='links',
                               primaryjoin=(Wordform.wordform_id == wordform_from))
    linked_to = relationship('Wordform', backref='links_2_to_1',
                             primaryjoin=(Wordform.wordform_id == wordform_to))

    def __init__(self, wf1, wf2):
        self.linked_from = wf1
        self.linked_to = wf2

    def __str__(self):
        return '<WordformLink {} -> {}>'.format(self.linked_from.wordform, self.linked_to.wordform)


#wordform_link = Table('wordform_links', Base.metadata,
#     Column('wordform_link_id', BIGINT(20), primary_key=True),
#     Column('wordform_1_id', BIGINT(20), ForeignKey('wordforms.wordform_id')),
#     Column('wordform_2_id', BIGINT(20), ForeignKey('wordforms.wordform_id'))
#     )


class WordformLinkSource(Base):
    __tablename__ = 'source_x_wordform_link'

    source_x_wordform_link_id = Column(BIGINT(20), primary_key=True)
    wordform_link_id = Column(BIGINT(20), ForeignKey('wordform_links.wordform_link_id'))
    lexicon_id = Column(BIGINT(20), ForeignKey('lexica.lexicon_id'))

    wordform_from_correct = Column(Boolean)
    wordform_to_correct = Column(Boolean)

    wfls_wflink = relationship('WordformLink', backref='wf_links')
    wfls_lexicon = relationship('Lexicon', backref='wfl_lexica')

    def __init__(self, wflink, wf_from_correct, wf_to_correct, lexicon):
        self.wfls_wflink = wflink
        self.wordform_from_correct = wf_from_correct
        self.wordform_to_correct = wf_to_correct
        self.wfls_lexicon = lexicon

    def __str__(self):
        return '<WordformLinkSource {} -> {} in "{}">'.format(self.wfls_wflink.linked_from.wordform, self.wfls_wflink.linked_to.wordform, self.wfls_lexicon.lexicon_name)


# source_x_wordform_link = Table('source_x_wordform_link', Base.metadata,
#                                Column('source_x_wordform_link_id', BIGINT(20), primary_key=True),
#                                Column('wordform_link_id', BIGINT(20), ForeignKey('wordform_links.wordform_link_id')),
#                                Column('lexicon_id', BIGINT(20), ForeignKey('lexica.lexicon_id'))
#                                )
