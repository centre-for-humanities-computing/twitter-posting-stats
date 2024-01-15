import glob
import gzip
from typing import Iterator

import dacy
import spacy
import jsonlines
from tqdm import tqdm


def iter_lines(fp: str) -> Iterator[str]:
    files = glob.glob(fp)
    for file in files:
        with gzip.open(file) if file.endswith('gz') else open(file) as f:
            for line in f:
                yield line


nlp = spacy.blank("da")  # an empty spacy pipeline
nlp.add_pipe("dacy/subjectivity")
nlp.add_pipe("dacy/polarity")
nlp.add_pipe("dacy/emotionally_laden")  # for emotianal/non-emotional
nlp.add_pipe("dacy/emotion")  # for type of emotion

print('Starting')
reader = jsonlines.Reader(iter_lines('output/test.ndjson/tweets/*'))
for doc, context in tqdm(nlp.pipe(((obj['text'], obj) for obj in reader), as_tuples=True)):
    pass
    # print(doc)
    # print(doc._.subjectivity)
    # print(doc._.subjectivity_prob)
    # print(doc._.polarity)
    # print(doc._.polarity_prob)
    # print(doc._.emotionally_laden)
    # # if emotional print the emotion
    # if doc._.emotionally_laden == "emotional":
    #     print("\t", doc._.emotion)


