#!/usr/bin/env python3
"""build_record_file.py - Process a collection file and write a temporary records file."""
import json
import os
import subprocess
import time

from utilities import CollectionProcessor, get_file_info


def build_record_file(args):
    """
    Build the record file.

    :param args: Program arguments
    """
    # Remove old record file if necessary
    if os.path.exists(args.record_file):
        os.remove(args.record_file)

    record_type = 'standard'
    if args.nextword:
        record_type = 'nextword'
    print(f'\nWriting temporary {record_type} record file')
    start_time = time.time()

    record_builder = RecordBuilder(args)
    record_builder.build()

    elapsed = time.time() - start_time
    print(f'Wrote temporary record file, time elapsed: {elapsed:.2f} seconds')


class RecordBuilder(CollectionProcessor):
    """The RecordBuilder reads a collection file and write a record file."""

    def __init__(self, args):
        """
        Initialize the record builder with program arguments.

        :param args: Program arguments
        """
        super().__init__(args, 'P')
        self.num_words = 0
        self.doc_id_digits = 0
        self.set_doc_id_digits()

    def set_doc_id_digits(self):
        """
        Read the last document id to quickly get the number of documents.

        This is necessary to set the doc_id_digits, which is used to format
        the records file so it can be sorted correctly.
        """
        with open(self.args.collection_file, 'rb') as fin:
            fin.seek(-6, os.SEEK_END)
            check_str = fin.read(6)
            while check_str != b'<P ID=':
                fin.seek(-7, os.SEEK_CUR)
                check_str = fin.read(6)
            num_docs_str = fin.readline().decode().strip()[:-1]
            self.doc_id_digits = len(num_docs_str)

    def build(self):
        """Build the records file."""
        # call super class read_docs to process the collection file
        self.read_docs(self.args.collection_file)

        # sort records
        with subprocess.Popen(['sort', self.args.record_file, '-o', self.args.record_file]) as proc:
            proc.wait()

        # build collection metadata
        metadata = {
            'number_of_docs': self.num_docs,
            'collection_size': self.num_words,
            'collection_file': get_file_info(self.args.collection_file),
            'stemmed': self.args.stem
        }
        metadata_json = json.dumps(metadata)
        sed = f'sed -i \'1s;^;{metadata_json}\\n;\' {self.args.record_file}'
        with subprocess.Popen(sed, shell=True) as proc:
            proc.wait()

    def process_docs(self, docs):
        """
        Write out documents to the record file.

        :param docs: Documents to write out.
        """
        with open(self.args.record_file, 'a') as fout:
            for doc_id, terms in docs:
                doc_id = '{doc_id:0{width}d}'.format(width=self.doc_id_digits, doc_id=doc_id)
                if not self.args.nextword:
                    self.write_record(fout, doc_id, terms)
                else:
                    self.write_nextword_record(fout, doc_id, terms)

    def write_record(self, fout, doc_id, terms):
        """
        Write a standard index record.

        :param fout: file to write to
        :param doc_id: document id
        :param terms: Terms to write
        """
        term_pos_dict = {}
        for idx, term in enumerate(terms):
            if not term_pos_dict.get(term):
                term_pos_dict[term] = []
            term_pos_dict[term].append(str(idx))
        for term, pos_list in term_pos_dict.items():
            pos_list_str = ','.join(pos_list)
            fout.write(f'{term},{doc_id},{pos_list_str}\n')
            self.num_words += len(pos_list)

    def write_nextword_record(self, fout, doc_id, terms):
        """
        Write a nextword index record.

        :param fout: file to write to
        :param doc_id: document id
        :param terms: Terms to write
        """
        term_pos_dict = {}
        for idx, first_term in enumerate(terms):
            next_term = '_'
            if idx + 1 != len(terms):
                next_term = terms[idx + 1]
            terms_tuple = (first_term, next_term)
            if not term_pos_dict.get(terms_tuple):
                term_pos_dict[terms_tuple] = []
            term_pos_dict[terms_tuple].append(str(idx))
        for (first_term, next_term), pos_list in term_pos_dict.items():
            pos_list_str = ','.join(pos_list)
            fout.write(f'{first_term},0,{next_term},{doc_id},{pos_list_str}\n')
            self.num_words += len(pos_list)
