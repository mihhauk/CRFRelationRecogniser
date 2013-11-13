# -*- coding: utf-8 -*-
import corpus2
import sys
import os
from multiprocessing import Process, Queue

NUM_THREADS = 6
MAX_CONTEXT_LEN = 5
tagset = corpus2.get_named_tagset('nkjp')
pairs = set()
output_dir = ''

class Consumer(Process):
    
    def __init__(self, task_queue):
        Process.__init__(self)
        self.task_queue = task_queue

    def run(self):
        while True:
            path = self.task_queue.get()
            if path is None:
                # Poison pill means we should exit
                break

            tok_reader = corpus2.TokenReader.create_path_reader('xces', tagset, path)
            writer = corpus2.TokenWriter.create_path_writer('ccl', os.path.join(output_dir,os.path.basename(path).replace('.xml.ccl', '.xml')), tagset)
            for sent in sentences(tok_reader):
                asent = reset_annotations(sent)
                context = check_sentence(asent, pairs)
                if context:
                    if not(context[1] - context[0] > MAX_CONTEXT_LEN or context[1] - context[0] == 1):
                        asent = annotate_context(asent, *context)
                        writer.write_sentence(corpus2.AnnotatedSentence.cast_as_sentence(asent))
                else:
                    writer.write_sentence(corpus2.AnnotatedSentence.cast_as_sentence(asent))
            del tok_reader
            del writer



def main(args):
    global output_dir
    output_dir = args[2]

    with open(args[0], 'r') as hyponym_pairs:
        for pair in hyponym_pairs.readlines():
            hyponym, hypernym = pair.strip().split(';')[::2]
            if hyponym != hypernym:
                pairs.add((hyponym, hypernym))

    

    files_to_process = Queue()   
    processes = [Consumer(files_to_process) for i in xrange(NUM_THREADS)]
    for p in processes:
        p.start()

    files_to_process = queue_files(args[1], '.xml.ccl', files_to_process)
    for i in xrange(NUM_THREADS):
        files_to_process.put(None)



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



def queue_files(path, extension, queue=Queue()):
    if os.path.isdir(path):
        path = path[:-1] if path[-1] == '/' else path
        docs = [f for f in os.listdir(path) if f.endswith(extension)]
        for filename in docs:
            queue.put('{0}/{1}'.format(path, filename))
        for subdir in os.walk(path).next()[1]:
            queue_files(os.path.join(path, subdir), extension, queue)
    else:
        raise Exception(path+' is not a valid directory')
    return queue



if __name__ == '__main__':
    main(sys.argv[1:])