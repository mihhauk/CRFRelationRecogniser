# -*- coding: utf-8 -*-
import corpus2
import sys
import os
from multiprocessing import Process, Queue
import time

NUM_THREADS = 6
MAX_CONTEXT_LEN = 5
tagset = corpus2.get_named_tagset('nkjp')
pairs = set()


def main(args):
    with open(args[0], 'r') as hyponym_pairs:
        for pair in hyponym_pairs.readlines():
            hyponym, hypernym = pair.strip().split(';')[::2]
            if hyponym != hypernym:
                pairs.add((hyponym, hypernym))

    files = list_files(args[1], '.xml.ccl')

    processed_sentences = [[] for i in xrange(NUM_THREADS if NUM_THREADS < len(files) else len(files))]

    # with open(os.path.join(args[2],'batch.txt'), 'w') as batch:
    #     batch.write(('\n').join(files))

    # positive_sentences, negative_sentences = [], []

    processes = [Process(target=process_file, args=(files.pop(), processed_sentences[i])) for i in xrange(NUM_THREADS if NUM_THREADS < len(files) else len(files))]

    for proc in processes:
        proc.start()

    while files:
        for idx, proc in enumerate(processes):
            if not proc.is_alive():
                processes[idx] = Process(target=process_file, args=(files.pop(), processed_sentences[idx]))
                processes[idx].start()

    while any(t.is_alive() for t in processes):
        time.sleep(2)

    writer = corpus2.TokenWriter.create_path_writer('ccl', os.path.join(args[2],'annotated_context.xml'), tagset)
    for sent in [item for sublist in processed_sentences for item in sublist]:
        writer.write_sentence(sent)
    del writer
        


def process_file(path, processed_sentences):
    # print 'processing', path 
    tok_reader = corpus2.TokenReader.create_path_reader('xces', tagset, path)
    for sent in sentences(tok_reader):
        asent = reset_annotations(sent)
        context = check_sentence(asent, pairs)
        if context:
            if not(context[1] - context[0] > MAX_CONTEXT_LEN or context[1] - context[0] == 1):
                asent = annotate_context(asent, *context)
                processed_sentences.append(corpus2.AnnotatedSentence.cast_as_sentence(asent))
        else:
            processed_sentences.append(corpus2.AnnotatedSentence.cast_as_sentence(asent))
    del tok_reader

def reset_annotations(sent):
    asent = corpus2.AnnotatedSentence.wrap_sentence(sent)
    for chan in asent.all_channels():
        asent.remove_channel(chan)
    asent.create_channel('relationcontext')
    return asent


def annotate_context(asent, start, end):
    chan = asent.get_channel('relationcontext')
    for idx in xrange(start + 1, end):
        chan.set_segment_at(idx, 1)
    return asent

def check_sentence(sent, pairs):
    """
    Checks sentence for pair of tokens in relation 
    """
    context = None
    lexemes = []
    for token in sent.tokens():
        lexemes.append(token.get_preferred_lexeme(tagset).lemma_utf8())
    for hyponym, hypernym in pairs:
        cur_start, cur_end = None, None
        for idx, lexeme in enumerate(lexemes):
            if hyponym == lexeme:
                if hypernym:
                    cur_start = idx
                    hyponym = None
                else:
                    cur_end = idx
            elif hypernym == lexeme:
                if hyponym:
                    cur_start = idx
                    hypernym = None
                else:
                    cur_end = idx
        if cur_start and cur_end:
            if context:
                print 'another context'
                if context[1] - context[0] < cur_end - cur_start:
                    context = (cur_start, cur_end)
            else:
                context = (cur_start, cur_end)
    return context    


def sentences(rdr):
    while True:
        sentence = rdr.get_next_sentence()
        if not sentence:
            break
        yield sentence



def list_files(path, extension, queue=Queue):
    if os.path.isdir(path):
        path = path[:-1] if path[-1] == '/' else path
        docs = [f for f in os.listdir(path) if f.endswith(extension)]
        for filename in docs:
            queue.put('{0}/{1}'.format(path, filename))
        for subdir in os.walk(path).next()[1]:
            list_files(os.path.join(path, subdir), extension, queue)
    else:
        raise Exception(path+' is not a valid directory')
    return queue



if __name__ == '__main__':
    main(sys.argv[1:])