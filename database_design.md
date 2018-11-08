# Database design

## Use cases

* Spelling normalization
	- Facilitates searching for spelling variants
* Spelling modernization
* OCR post correction
	- Get text as close to the original text as possible
	- Get modernized/normalized text
* Spelling correction

## Example/benchmark queries

* Aggregate over words in corpus (or arbitrary selection of texts)
	- Word frequency list
	- Tf idf(?)
	- ... (we need some realistic stuff here!)
* Text reuse(?) (probably not realistic)
	- Find reused passages (of certain length) in other texts
* Find duplicate documents(?)

## Requirements

* Store anagram hashes of word forms
	- Makes it possible to link
	- Makes it possible to find all occurring wordforms for a given anagram value
* Store all possible anagram values of a wordform for a given alphabet and edit distance
	- Makes it possible to link wordforms if a new wordform is encountered/added to the database

## Brainstorm

* Information about the ocr software and process (impossible) (document-level)
* Information about quality of ocr text (document-level)
