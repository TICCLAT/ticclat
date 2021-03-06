# coding: utf-8
# pylint: disable=too-few-public-methods
"""
SQLAlchemy schema of the TICCLAT database.

Contains all the tables of the database and their connections, defined as
SQLAlchemy declarative_base subclasses.

Many of the tables here defined are based on an INT lexicon database created
in the IMPACT project
(https://ivdnt.org/images/stories/onderzoek_en_onderwijs/publicaties/impact/impact_lexicon_structure.pdf).
See https://github.com/TICCLAT/docs/blob/master/database_design.md for more
information about the database design.

Based on this, in TICCLAT, we added tables for:
- links between wordforms
- morphological paradigm groups of wordforms
- anagram hashes from TICCL
- spelling variants from TICCL
- identifiers linking wordforms to external sources like the WNT, MNW, INT.
"""

from sqlalchemy import Column, String, Table, ForeignKey, Unicode, Boolean, \
    Integer, BigInteger, ForeignKeyConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


# Table for storing the relation between corpora and documents.
corpusId_x_documentId = Table('corpusId_x_documentId', Base.metadata,
                              Column('corpus_id', BigInteger(), ForeignKey('corpora.corpus_id')),
                              Column('document_id', BigInteger(), ForeignKey('documents.document_id'))
                              )


class TextAttestation(Base):
    """
    Table for storing text attestations.

    A text attestation entry is defined in the INT schema as the occurrence
    and frequency of wordforms in documents.
    """
    __tablename__ = 'text_attestations'

    attestation_id = Column(BigInteger(), primary_key=True)
    frequency = Column(BigInteger())
    wordform_id = Column(BigInteger(), ForeignKey('wordforms.wordform_id'))
    document_id = Column(BigInteger(), ForeignKey('documents.document_id'))

    ta_document = relationship('Document', back_populates='document_wordforms')
    ta_wordform = relationship('Wordform', back_populates='wordform_documents')

    def __init__(self, document, wordform, frequency):
        self.ta_document = document
        self.ta_wordform = wordform
        self.frequency = frequency


class Corpus(Base):
    """Table for storing corpus metadata."""
    __tablename__ = 'corpora'

    corpus_id = Column(BigInteger(), primary_key=True)
    name = Column(String(255))
    corpus_documents = relationship('Document',
                                    secondary=corpusId_x_documentId,
                                    back_populates='document_corpora')


class Document(Base):
    """Table for storing document metadata."""
    __tablename__ = 'documents'

    document_id = Column(BigInteger(), primary_key=True)
    persistent_id = Column(String(255), index=True)
    word_count = Column(BigInteger())
    encoding = Column(BigInteger())
    title = Column(String(255))
    year_from = Column(BigInteger())
    year_to = Column(BigInteger())
    pub_year = Column(BigInteger())
    author = Column(String(255))
    editor = Column(String(255))
    publisher = Column(String(255))
    publishing_location = Column(String(255))
    text_type = Column(String(255))
    region = Column(String(255))
    language = Column(String(255))
    other_languages = Column(String(255))
    spelling = Column(String(255))
    parent_document = Column(BigInteger(), index=True)

    document_corpora = relationship('Corpus', secondary=corpusId_x_documentId,
                                    back_populates='corpus_documents')
    document_wordforms = relationship('TextAttestation', back_populates='ta_document')


# Table for storing which lexica wordforms occur in.
lexical_source_wordform = Table('lexical_source_wordform', Base.metadata,
                                Column('wordform_source_id', BigInteger(), primary_key=True),
                                Column('lexicon_id', BigInteger(), ForeignKey('lexica.lexicon_id')),
                                Column('wordform_id', BigInteger(), ForeignKey('wordforms.wordform_id'))
                                )


class Lexicon(Base):
    """
    Table for storing lexicon metadata.

    vocabulary (bool): if True, all words in this lexicon are (supposed to be)
                       valid words, if False, some are misspelled
    """
    __tablename__ = 'lexica'

    lexicon_id = Column(BigInteger(), primary_key=True)
    lexicon_name = Column(String(255))
    vocabulary = Column(Boolean)

    lexicon_wordforms = relationship('Wordform',
                                     secondary=lexical_source_wordform,
                                     back_populates='wf_lexica')
    lexicon_wordform_links = relationship('WordformLink',
                                          secondary='source_x_wordform_link')

    def __str__(self):
        return '<Lexicon {}>'.format(self.lexicon_name)


class Anahash(Base):
    """
    Table for storing anahashes.

    The anahashes in this table have no direct relation to the wordforms, those
    links are tracked in the wordforms table. This was done so that the
    anahashes table can be efficiently searched, e.g. for ranges in anahash
    "space".
    """
    __tablename__ = 'anahashes'

    anahash_id = Column(BigInteger(), primary_key=True)
    anahash = Column(BigInteger(), unique=True, index=True)

    def __str__(self):
        return '<Anahash {}>'.format(self.anahash)


class Wordform(Base):
    """Table for storing wordforms and associated anahashes."""
    __tablename__ = 'wordforms'

    wordform_id = Column(BigInteger(), primary_key=True)
    wordform = Column(Unicode(255, convert_unicode=False), unique=True, index=True)
    anahash_id = Column(BigInteger(), ForeignKey("anahashes.anahash_id", ondelete='SET NULL'))

    anahash = relationship('Anahash')
    wordform_lowercase = Column(Unicode(255, convert_unicode=False), nullable=False, index=True)

    wf_lexica = relationship('Lexicon', secondary=lexical_source_wordform,
                             back_populates='lexicon_wordforms')
    wordform_documents = relationship('TextAttestation', back_populates='ta_wordform')

    def link(self, wordform):
        """Add WordformLinks between self and another wordfrom and vice versa.

        The WordformLinks are added only in the link does not yet exist.

        Inputs:
            wordform (Wordform): Wordform that is related to Wordform self.
        """
        links = [w.linked_to for w in self.links]
        if wordform not in links:
            WordformLink(self, wordform)
            WordformLink(wordform, self)

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
        wflinks = wfl.wf_links
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
    """Table for storing links between wordforms."""
    __tablename__ = 'wordform_links'

    wordform_from = Column(BigInteger(), ForeignKey('wordforms.wordform_id'), primary_key=True)
    wordform_to = Column(BigInteger(), ForeignKey('wordforms.wordform_id'), primary_key=True)

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
    """
    Table for storing the sources of links between wordforms.

    Wordform links are given by lexica (dictionaries, spelling correction
    lists, etc.). This table records which lexicon a given link between
    wordforms was originally ingested from.
    """
    __tablename__ = 'source_x_wordform_link'
    __table_args__ = (
        ForeignKeyConstraint(['wordform_from', 'wordform_to'],
                             ['wordform_links.wordform_from', 'wordform_links.wordform_to']),
    )

    source_x_wordform_link_id = Column(BigInteger(), primary_key=True)
    wordform_from = Column(BigInteger(), nullable=False)
    wordform_to = Column(BigInteger(), nullable=False)
    lexicon_id = Column(BigInteger(), ForeignKey('lexica.lexicon_id'))

    wordform_from_correct = Column(Boolean)
    wordform_to_correct = Column(Boolean)

    ld = Column(Integer())
    anahash_difference = Column(BigInteger())

    wfls_wflink = relationship('WordformLink', backref='wf_links')
    wfls_lexicon = relationship('Lexicon', backref='wfl_lexica')

    def __init__(self, wflink, wf_from_correct, wf_to_correct, lexicon):
        self.wfls_wflink = wflink
        self.wordform_from_correct = wf_from_correct
        self.wordform_to_correct = wf_to_correct
        self.wfls_lexicon = lexicon

    def __str__(self):
        return '<WordformLinkSource {} -> {} in "{}">'.format(
            self.wfls_wflink.linked_from.wordform,
            self.wfls_wflink.linked_to.wordform,
            self.wfls_lexicon.lexicon_name,
        )


class MorphologicalParadigm(Base):
    """
    Table for storing information about morphological paradigms of wordforms.

    The paradigms are determined according to Reynaert's method (to be
    published).
    """
    __tablename__ = 'morphological_paradigms'

    paradigm_id = Column(BigInteger(), primary_key=True)

    Z = Column(BigInteger(), index=True)
    Y = Column(BigInteger(), index=True)
    X = Column(BigInteger(), index=True)
    W = Column(BigInteger(), index=True)
    V = Column(BigInteger(), index=True)
    word_type_code = Column(String(10), index=True)
    word_type_number = Column(BigInteger(), index=True)

    wordform_id = Column(BigInteger(), ForeignKey('wordforms.wordform_id'))


class ExternalLink(Base):
    """Table for storing ids from external sources of wordforms.

    Used for linking wordforms to external sources, such as the WNT, MNW, INT.
    """
    __tablename__ = 'external_links'

    external_link_id = Column(BigInteger(), primary_key=True)
    wordform_id = Column(BigInteger(), ForeignKey('wordforms.wordform_id'))
    source_name = Column(String(5))
    source_id = Column(String(10))


class WordformFrequencies(Base):
    """Materialized view containing overall frequencies of wordforms

    The data in this table can be used to filter wordforms on frequency. This
    is necessary, because there is a lot of noise in the wordforms table, and
    this makes aggregating over all wordforms expensive.
    """
    __tablename__ = 'wordform_frequency'

    wordform_id = Column(BigInteger(), primary_key=True)
    wordform = Column(Unicode(255, convert_unicode=False), index=True,
                      unique=True)
    frequency = Column(BigInteger())


class TicclatVariant(Base):
    """Contains spelling variants of words, ingested from TICCL
    """
    __tablename__ = 'ticcl_variants'

    ticclat_variant_id = Column(BigInteger(), primary_key=True)
    wordform = Column(Unicode(255, convert_unicode=False), index=True, unique=True)
    wordform_source = Column(Unicode(255, convert_unicode=False), index=True)
    wordform_source_id = Column(BigInteger(), ForeignKey('wordforms.wordform_id'), index=True)
    levenshtein_distance = Column(BigInteger(), index=True)
    frequency = Column(BigInteger(), index=True)
