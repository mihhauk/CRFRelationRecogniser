# -*- coding: utf-8 -*-
"""
Split annotated data to training and test set(s).
Usage:
    split_data.py by (documents | sentences) <src-dir> percentage <train-pct> [options]
    split_data.py by (documents | sentences) <src-dir> equal <numfolds> [options]

    -o=DIR, --output=DIR    output dir [default: <src-dir>]
"""
from docopt import docopt
import corpus2
import os
from random import shuffle
import numpy as np
from corpus_merge import merge

tagset = corpus2.get_named_tagset('nkjp')


def main():
    args = docopt(__doc__)
    print args
    out_dir = args['--output'] if args['--output'] != '<src-dir>' else args['<src-dir']
    if args['percentage']:
        args['<train-pct>'] = int(args['<train-pct>'])  # replace with Schema package
    else:
        args['<numfolds>'] = int(args['<numfolds>'])
    positive, negative = [], []
    for path in list_files(args['<src-dir>']):
        file_positive, file_negative = check_file_examples(path, args['sentences'])
        positive += file_positive
        negative += file_negative
    shuffle(positive)
    shuffle(negative)
    positive = np.array(positive)
    negative = np.array(negative)
    print len(positive), len(negative)
    print type(positive), type(negative)
    if args['percentage']:
        print type(args['<train-pct>'])
        args['<train-pct>'] /= 100.0
        train_pos, test_pos = np.array_split(positive, [len(positive) * args['<train-pct>']])
        train_neg, test_neg = np.array_split(negative, [len(negative) * args['<train-pct>']])
        train_set = np.concatenate([train_pos, train_neg])
        shuffle(train_set)
        test_set = np.concatenate([test_pos, test_neg])
        shuffle(test_set)
        if args['sentences']:
            write_to_file(os.path.join(out_dir, 'train.xml'), train_set)
            write_to_file(os.path.join(out_dir, 'test.xml'), test_set)
        else:
            merge(train_set, os.path.join(args['<src-dir>'], 'train.xml'),
                  tagset=tagset, documents_as_chunks=True)
            merge(test_set, os.path.join(args['<src-dir>'], 'test.xml'),
                  tagset=tagset, documents_as_chunks=True)
    elif args['equal']:
        pos_folds = np.array_split(positive, args['<numfolds>'])
        neg_folds = np.array_split(negative, args['<numfolds>'])
        for idx, (pos, neg) in enumerate(zip(pos_folds, neg_folds)):
            fold = np.concatenate([pos, neg])
            shuffle(fold)
            if args['sentences']:
                write_to_file(os.path.join(out_dir, 'fold{0}.xml'.format(idx)), fold)
            else:
                merge(fold, os.path.join(args['<src-dir>'], 'fold{0}.xml'.format(idx)),
                      tagset=tagset, documents_as_chunks=True)


def check_file_examples(path, split_by_sentences):
    positive, negative = [], []
    rdr = corpus2.TokenReader.create_path_reader('ccl', tagset, path)
    for sent in get_sentences(rdr):
        asent = corpus2.AnnotatedSentence.wrap_sentence(sent)
        if asent.has_channel('relationcontext'):
            if split_by_sentences:
                positive.append(sent)
            else:
                return [path], []
        elif split_by_sentences:
            negative.append(sent)
    del rdr
    if split_by_sentences:
        return positive, negative
    else:
        return [], [path]


def write_to_file(path, sentences):
    writer = corpus2.TokenWriter.create_path_writer('ccl', path, tagset)
    for sent in sentences:
        writer.write_sentence(sent)
    del writer


def get_sentences(rdr):
    while True:
        sent = rdr.get_next_sentence()
        if not sent:
            break
        yield sent


def list_files(path, extension='.xml'):
    docs = [f for f in os.listdir(path) if f.endswith(extension)]
    for filename in docs:
        yield '{0}/{1}'.format(path, filename)

if __name__ == '__main__':
    main()
