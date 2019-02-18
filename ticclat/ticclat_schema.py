# coding: utf-8
from sqlalchemy import Column, String, Table, ForeignKey, Unicode, Boolean, \
    Integer
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import BIGINT
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

    def __init__(self, document, wordform, frequency):
        self.ta_document = document
        self.ta_wordform = wordform
        self.frequency = frequency


class Corpus(Base):
    __tablename__ = 'corpora'

    corpus_id = Column(BIGINT(20), primary_key=True)
    name = Column(String(255))
    corpus_documents = relationship('Document',
                                    secondary=corpusId_x_documentId,
                                    back_populates='document_corpora')

    def add_document(self, terms_vector, wfs):
        """Add a document to the Corpus.

        Inputs:
            terms_vector (Counter): term-frequency vector representing the
                document.
            wfs (list of Wordforms): list of Wordforms that occur in the
                document

        Returns:
            Document (Document object): The document that was added to the
                corpus
        """
        # TODO: Fix setting document metadata
        d = Document(word_count=sum(terms_vector.values()))
        for wf in wfs:
            TextAttestation(d, wf, terms_vector[wf.wordform])

        d.document_corpora.append(self)

        return d


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

    wordform_id = Column(BIGINT(20).with_variant(Integer, 'sqlite'), primary_key=True)
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

        Inputs:
            wf (Wordform): Wordform that is related to Wordform self.
        """
        links = [w.linked_to for w in self.links]
        if wf not in links:
            WordformLink(self, wf)
            WordformLink(wf, self)

    def link_with_metadata(self, wf_to, wf_from_correct, wf_to_correct,
                           lexicon):
        """Add WordformLinks with metadata.

        Adds a WordformLink between self and another wordfrom, and vice versa,
        if these links are not yet in the database.
        And adds a WordformLinkSource, with Lexicon, and information about
        which Wordforms are correct according to the Lexicon. No duplicate
        WordformLinkSources are added.

        TODO: add Uniqueconstraint on (wf_from (self), wf_to, lexicon)?

        Inputs:
            wf_to (Wordform): Wordform self will be linked to (and vice versa)
            wf_from_correct (boolean): True if Wordform self is correct
                according to the lexicon, False otherwise.
            wf_to_correct (boolean): True if Wordform wf_to is correct
                according to the lexicon, False otherwise.
            lexicon (Lexicon): The Lexicon that contains the WordformLink
        """
        self.link(wf_to)

        # check whether the WordformLinkSource is already in the database
        wfl = next((wfl for wfl in self.links if wfl.linked_to == wf_to))
        wflinks = [link for link in wfl.wf_links]
        lexica = [l.wfls_lexicon for l in wflinks]

        if lexicon not in lexica:
            # add WordformLinkSource for link from wf (self) to corr
            WordformLinkSource(wfl, wf_from_correct, wf_to_correct, lexicon)

            # add WordformLinkSource for link from corr to wf (self)
            wfl = next((wfl for wfl in wf_to.links if wfl.linked_to == self))
            WordformLinkSource(wfl, wf_to_correct, wf_from_correct, lexicon)

    def link_spelling_correction(self, corr, lexicon):
        """Add a spelling correction WordformLink.

        This method sets the booleans that indicate which Wordforms are correct
        (according to the lexicon).

        Inputs:
            corr (Wordform): A correction candidate of Wordform self
            lexicon (Lexicon): The Lexicon that contains the WordformLink
        """
        self.link_with_metadata(corr,
                                wf_from_correct=False,
                                wf_to_correct=True,
                                lexicon=lexicon)

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
