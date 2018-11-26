# TICCL

Notes on using TICCL.

## Run TICCL locally in Docker

Get the LaMachine Docker image:

```
docker pull proycon/lamachine:latest
```

Run the Docker (with volume mapping for using your own data):

```
docker run -p 8080:80 -t -i -v /path/to/data:/data proycon/lamachine
```

Install TICCL in the Docker container:

```
lamachine-update --edit
```

Save the first file that is opened. Uncomment nextflow and piccl in the next
file that is opened and save. Wait.

Now you can run the TICCL modules (I had to install PICCL twice before it worked).

## TICCL modules

| Module | Output | Explanantion |
| ------ | ------ | ----------- |
| `folia-stats`/`TICCL-stats`   | word frequency list  | folia-stats expects folia files as inputs (with a text field), TICCL-stats expects txt files. The module splits words on whitespace, attached punctuation is removed later. Can also calculate frequencies of ngrams. |
| `TICCL-unk`   | *.punc, *.clean, *.unk  | Clean the frequency lists. This is where something like tokenization happens (removal of punctuation). Words shorter than 6 characters and 'junk' are stored in *.unk  |
| `TICCL-anahash`   | *.conf  | Calculate anagram value for bag of characters. On average 1.2 anagrams per word (most words don't have anagrams)  |
