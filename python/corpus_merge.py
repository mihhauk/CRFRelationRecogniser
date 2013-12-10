# -*- coding: utf-8 -*-
import corpus2
import os
from xml.sax.saxutils import escape


def merge(source_paths, output_path=None, input_format='ccl', output_format='ccl', tagset='nkjp',
          chunks=False, prefix_chunks=False, prefix_sentences=False, documents_as_chunks=False):
    # load a tagset, create a reader
    if isinstance(tagset, str):
        tagset = corpus2.get_named_tagset(tagset)
    if output_path:
        writer = corpus2.TokenWriter.create_path_writer(output_format, output_path, tagset)
    else:
        writer = corpus2.TokenWriter.create_stdout_writer(output_format, tagset)
    for path in source_paths:
        reader = corpus2.TokenReader.create_path_reader(input_format, tagset, path)
        fname, _ = os.path.splitext(os.path.basename(path))
        fname = escape(fname)
        if chunks:
            chunk_no = 1
            for chunk in chunks(reader):
                if prefix_chunks:
                    if chunk.has_attribute('id'):
                        their_id = chunk.get_attribute('id')
                    else:
                        # autogen
                        their_id = ('auto%03d' % chunk_no)
                    full_id = 'file:%s:%s' % (fname, their_id)
                    chunk.set_attribute('id', full_id)
                writer.write_chunk(chunk)
                chunk_no += 1
        else:
            big_chunk = None
            if documents_as_chunks:
                big_chunk = corpus2.Chunk()
                big_chunk.set_attribute('id', 'file:%s:%s' % (fname, 'ch1'))
            sent_no = 1
            for sent in sentences(reader):
                if prefix_sentences:
                    if not sent.id():
                        their_id = sent.id()
                    else:
                        #autogen
                        their_id = ('s%d' % sent_no)
                    full_id = 'file:%s:%s' % (fname, their_id)
                    sent.set_id(full_id)
                if big_chunk:
                    big_chunk.append(sent)
                else:
                    writer.write_sentence(sent)
                sent_no += 1
            if big_chunk:
                writer.write_chunk(big_chunk)
        del reader


def sentences(rdr):
    """Yields subsequent sentences from a reader."""
    while True:
        sent = rdr.get_next_sentence()
        if not sent:
            break
        yield sent


def chunks(rdr):
    """Yields subsequent sentences from a reader."""
    while True:
        chunk = rdr.get_next_chunk()
        if not chunk:
            break
        yield chunk
