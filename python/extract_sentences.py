# -*- coding: utf-8 -*-
import corpus2
import sys
import os
from multiprocessing import Process, Queue
import itertools

NUM_THREADS = 6
MAX_CONTEXT_LEN = 10
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
                contexts = check_sentence(asent, pairs)
                if contexts is not None:
                    # print contexts
                    asent = annotate_context(asent, contexts)
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


def annotate_context(asent, contexts):
    chan = asent.get_channel('relationcontext')
    for nr, (start, end) in enumerate(contexts, start=1):
        for idx in xrange(start, end + 1):
            chan.set_segment_at(idx, nr)
    return asent

def check_sentence_old(sent, pairs):
    """
    Checks sentence for pair of tokens in relation 
    """
    contexts = []
    lexemes = []
    for token in sent.tokens():
        lexemes.append(token.get_preferred_lexeme(tagset).lemma_utf8())  

    # print '>>>>>>>'   
    for hyponym, hypernym in pairs:
        cur_start, cur_end = None, None
        for idx, lexeme in enumerate(lexemes):
            if hyponym == lexeme:
                if hypernym:
                    cur_start = idx + 1
                    hyponym = None
                else:
                    cur_end = idx - 1
            elif hypernym == lexeme:
                if hyponym:
                    cur_start = idx + 1
                    hypernym = None
                else:
                    cur_end = idx - 1
            if valid_context(cur_start, cur_end):
                # print 'valid', cur_start, cur_end
                # print 'context', contexts
                if not overlaps(cur_start, cur_end, contexts):
                    # print 'not overlaps'
                    contexts.append((cur_start, cur_end))
                else:
                    # print 'overlaps', contexts
                    return None
            # elif cur_start is not None and cur_end is not None:
                # print 'invalid', cur_start, cu
    return contexts  

def check_sentence(sent, pairs):
    """
    Checks sentence for pair of tokens in relation 
    """
    contexts = []
    lexemes = []
    i = 0
    for token in sent.tokens():
        lexemes.append((token.get_preferred_lexeme(tagset).lemma_utf8(), i))
        i += 1

    for ((word1, id1), (word2, id2)) in itertools.combinations(lexemes, 2):
        if (word1, word2) in pairs or (word2, word1) in pairs:
            if (1 < abs(id1 - id2) < MAX_CONTEXT_LEN):
                if id1 < id2:
                    cur_start = id1
                    cur_end = id2
                else:
                    cur_start = id2
                    cur_end = id1   
                if not overlaps(cur_start, cur_end, contexts):
                    contexts.append((cur_start + 1, cur_end - 1))
                else:
                    return None              
    return contexts  



def overlaps(start, end, contexts):
    cur_context = range(start, end+1)
    for context in contexts:
        if any(idx in cur_context for idx in context):
            return True
    return False

def valid_context(start, end):
    return start and end and start != end and end - start <= MAX_CONTEXT_LEN

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