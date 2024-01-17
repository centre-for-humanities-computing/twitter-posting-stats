import argparse
import glob
import gzip
import os.path
import sys
from typing import Iterator

import dacy
import spacy
import jsonlines
from tqdm import tqdm

from postingstats.datamodels import MinimalTweet


def iter_lines(fp: str) -> Iterator[str]:
    files = glob.glob(fp)
    for file in files:
        with gzip.open(file) if file.endswith('gz') else open(file) as f:
            for line in f:
                yield line


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("input_path",
                        help="Input path of jsonlines file which can also be gzipped"
                             "and/or split into multiple files. Be mindful of quotes"
                             "if you use a glob pattern for multiple files.")
arg_parser.add_argument("--output-path",
                        default=None,
                        required=False,
                        help="Output path of jsonlines output.")
args = arg_parser.parse_args()

input_path = args.input_path
if args.output_path is None:
    output_path = os.path.join('output', input_path, 'nlp.jsonl')
    print("Output path will be", output_path)
else:
    output_path = args.output_path

print('Starting ...')
classifications = ["subjectivity", "polarity", "emotionally_laden", "emotion"]
nlp = spacy.blank("da")  # an empty spacy pipeline
for classification in classifications:
    nlp.add_pipe(f"dacy/{classification}")

reader = jsonlines.Reader(iter_lines(input_path), loads=MinimalTweet.from_json)
doc_pipe = nlp.pipe(((tweet.text, tweet.id) for tweet in reader), as_tuples=True)
with open(output_path, 'w+') as out:
    writer = jsonlines.Writer(out)
    for doc, tweet_id in tqdm(doc_pipe, desc="Processing tweets"):
        output_dict = {"id": tweet_id}
        for extension in classifications:
            output_dict[extension] = getattr(doc._, extension)
            prob_ext = extension + '_prob'
            probs = getattr(doc._, prob_ext)
            output_dict[prob_ext] = {
                label: float(probs['prob'][i])
                for i, label in enumerate(probs['labels'])
            }
        writer.write(output_dict)



