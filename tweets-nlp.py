import argparse
import glob
import gzip
import os.path
import queue
import threading
from typing import Iterator

import spacy
import jsonlines
from spacy import Language
from spacy.tokens import Doc
from tqdm import tqdm

CLASSIFICATIONS = ("subjectivity", "polarity", "emotionally_laden", "emotion")


def iter_lines(fp: str) -> Iterator[str]:
    files = glob.glob(fp)
    for file in files:
        with gzip.open(file) if file.endswith('gz') else open(file) as f:
            for line in f:
                yield line


def reader_thread(input_path, processing_queue, batch_size=1000):
    reader = jsonlines.Reader(iter_lines(input_path))
    batch = []
    for obj in tqdm(reader):
        batch.append((obj['text'], obj['id']))
        if len(batch) >= batch_size:
            processing_queue.put(batch)
            batch = []
    processing_queue.put(batch)
    processing_queue.put(None)  # Sentinel to signify end of data


def get_classifications(doc: Doc, tweet_id: str):
    output_dict = {"id": tweet_id}
    for extension in CLASSIFICATIONS:
        output_dict[extension] = getattr(doc._, extension)
        prob_ext = extension + '_prob'
        probs = getattr(doc._, prob_ext)
        output_dict[prob_ext] = {
            label: float(probs['prob'][i])
            for i, label in enumerate(probs['labels'])
        }
    return output_dict


def worker_thread(nlp: Language, processing_queue, writing_queue, batch_size=1000):
    while True:
        batch = processing_queue.get()
        if batch is None:
            writing_queue.put(None)  # Pass the sentinel to the writer thread
            break
        processed_data = [get_classifications(*result)
                          for result in nlp.pipe(batch, as_tuples=True,
                                                 batch_size=batch_size)]
        writing_queue.put(processed_data)
        processing_queue.task_done()


def writer_thread(output_path, writing_queue):
    with open(output_path, 'w+') as out:
        writer = jsonlines.Writer(out)
        while True:
            processed_batch = writing_queue.get()
            if processed_batch is None:
                break
            writer.write_all(processed_batch)
            writing_queue.task_done()


def main(input_path: str, output_path: str, batch_size=1000):
    if output_path is None:
        output_path = os.path.join('output', input_path, 'nlp.jsonl')
        print("Output path will be", output_path)

    uses_gpu = spacy.prefer_gpu()
    if uses_gpu:
        print('Using GPU!')
    else:
        print('Not using GPU!')

    print('Starting ...')
    nlp = spacy.blank("da")  # an empty spacy pipeline
    for classification in CLASSIFICATIONS:
        nlp.add_pipe(f"dacy/{classification}")

    processing_queue = queue.Queue(maxsize=10)
    writing_queue = queue.Queue()

    # Start the reader thread
    t_reader = threading.Thread(
        target=reader_thread,
        args=(input_path, processing_queue, batch_size)
    )
    t_reader.start()

    t_worker = threading.Thread(
        target=worker_thread,
        args=(nlp, processing_queue, writing_queue, batch_size)
    )
    t_worker.start()

    # Start the writer thread
    t_writer = threading.Thread(
        target=writer_thread,
        args=(output_path, writing_queue)
    )
    t_writer.start()

    t_reader.join()
    t_worker.join()
    t_writer.join()
    processing_queue.join()
    writing_queue.join()


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("input_path",
                            help="Input path of jsonlines file which can also be gzipped"
                                 "and/or split into multiple files. Be mindful of quotes"
                                 "if you use a glob pattern for multiple files.")
    arg_parser.add_argument("--output-path",
                            default=None,
                            required=False,
                            help="Output path of jsonlines output.")
    arg_parser.add_argument("--batch_size",
                            default=1000,
                            type=int,
                            help="Size of document batching that get queued.")
    args = arg_parser.parse_args()
    main(args.input_path, args.output_path, args.batch_size)
