#!/usr/bin/env python3
"""build_index.py - Process a record file and write a lexicon and inverted file."""
import json
import pickle
import time

from utilities import INT_BYTES, get_file_info


def build_index(args):
    """
    Build the inverted file and lexicon file.

    :param args: Program arguments
    """
    print('\nWriting lexicon/inverted file')
    start_time = time.time()

    index_builder = IndexBuilder(args)
    index_builder.build()

    elapsed = time.time() - start_time
    print(f'Wrote lexicon/inverted file, time elapsed: {elapsed:.2f} seconds\n')


class IndexBuilder:
    """The LexiconBuilder reads a records file and write a lexicon and inverted file."""

    def __init__(self, args):
        """
        Initialize the LexiconBuilder with program arguments.

        :param args: Program arguments
        """
        self.args = args
        self.lexicon = {'metadata': {}, 'terms': {}, 'term_ids': {}}
        self.term_ids_inv = {}
        self.current_offset = 0
        self.inverted_file = open(self.args.inverted_file, 'wb')
        self.prev_next_term = None
        self.current_first_term = None

    def build(self):
        """Build the lexicon and inverted file."""
        with open(self.args.record_file, 'r') as fin:
            self.lexicon['metadata'] = json.loads(fin.readline())

            for line in fin:
                if not self.args.nextword:
                    self.process_record(line)
                else:
                    self.process_nextword_record(line)
        if self.args.nextword:
            self.write_prev_next_term()

        # set some metadata
        metadata = self.lexicon['metadata']
        metadata['inverted_file'] = get_file_info(self.args.inverted_file)
        metadata['record_file'] = get_file_info(self.args.record_file)
        metadata['vocabulary_size'] = len(self.lexicon['terms'])

        # write the lexicon to disk
        with open(self.args.lexicon_file, 'wb') as fout:
            pickle.dump(self.lexicon, fout)

    def process_record(self, record):
        """
        Process a single record.

        :param record: line from record file to process.
        """
        # extract record data
        record = record.split(',')
        term = record[0]
        doc_id = int(record[1])
        term_pos_list = [int(pos) for pos in record[2:]]
        term_freq = len(term_pos_list)

        # add new term to lexicon if needed
        terms = self.lexicon['terms']
        if not terms.get(term):
            terms[term] = {
                'doc_freq': 0,
                'collection_freq': 0,
                'offset': self.current_offset
            }

        # Update lexicon with record
        terms[term]['doc_freq'] += 1
        terms[term]['collection_freq'] += term_freq

        # write to inverted file and update offset
        self.write_int(doc_id)
        self.write_int(term_freq)
        for pos in term_pos_list:
            self.write_int(pos)

    def process_nextword_record(self, record):
        """
        Process a single nextword record.

        :param record: line from record file to process.
        """
        # extract record data
        record = record.split(',')
        first_term = record[0]
        next_term = record[2]
        doc_id = int(record[3])
        term_pos_list = [int(pos) for pos in record[4:]]
        term_freq = len(term_pos_list)

        terms = self.lexicon['terms']

        # add to term ids if needed
        term_ids = self.lexicon['term_ids']
        for term in [first_term, next_term]:
            if self.term_ids_inv.get(term) is None:
                term_id = len(term_ids)
                term_ids[term_id] = term
                self.term_ids_inv[term] = term_id

        # add new term to lexicon if needed
        if not terms.get(first_term):
            if self.prev_next_term is not None:
                self.write_prev_next_term()
            self.prev_next_term = {
                'term': term,
                'doc_ids': {}
            }
            self.current_first_term = first_term
            terms[first_term] = {
                'doc_freq': 0,
                'num_next_terms': 0,
                'collection_freq': 0,
                'offset': self.current_offset
            }

        # Update lexicon with record
        terms[first_term]['doc_freq'] += 1
        terms[first_term]['collection_freq'] += term_freq

        if next_term == self.prev_next_term['term']:
            self.prev_next_term['doc_ids'][doc_id] = term_pos_list
        else:
            self.write_prev_next_term()
            self.prev_next_term = {
                'term': next_term,
                'doc_ids': {}
            }
            self.prev_next_term['doc_ids'][doc_id] = term_pos_list

    def write_prev_next_term(self):
        """Write out the last next term that was processed."""
        self.lexicon['terms'][self.current_first_term]['num_next_terms'] += 1
        next_term_id = self.term_ids_inv[self.prev_next_term['term']]

        self.write_int(next_term_id)  # write next term id
        self.write_int(len(self.prev_next_term['doc_ids']))  # write first/next pair num docs
        for doc_id_i, pos_list in self.prev_next_term['doc_ids'].items():
            self.write_int(doc_id_i)  # write first/next pair doc id
            self.write_int(len(pos_list))  # write first/next pair doc freq
            for pos in pos_list:
                self.write_int(pos)  # write first/next doc postings list

    def write_int(self, integer):
        """
        Write integer to inverted file.

        :param integer: integer to write
        """
        self.inverted_file.write(integer.to_bytes(INT_BYTES, byteorder='big', signed=False))
        self.current_offset += 1
