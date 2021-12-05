#!/usr/bin/env python3
"""utilities.py - Common routines used throughout the project."""
import multiprocessing as mp
import os
import pprint
import re
from nltk.stem import SnowballStemmer

INT_BYTES = 4


class CollectionProcessor:
    """
    The CollectionProcessor is an abstract class that reads a collection of document.

    The base class should implement the process_docs() method, which is called by the
    CollectionProcessor when a list of documents are ready to be processed.
    """

    STEMMER = SnowballStemmer('english')
    NUM_DOCS_TO_READ_IN_MEM = 1000000

    def __init__(self, args, html_tag):
        """
        Initialize the CollectionProcessor with program arguments and an html tag.

        :param args: Program arguments
        :param html_tag: Collection file html tag (P or Q)
        """
        self.args = args
        self.html_tag = html_tag
        self.num_docs = 0
        self.end_of_file = False

    def process_docs(self, docs):
        """
        Implement in the base class to process available documents.

        :param docs: Documents ready to process

        :throws Exception: If unimplemented
        """
        raise Exception("Base class must implement process_docs()")

    def read_docs(self, collection_file):
        """
        Call from the base class to start reading the collection file.

        :param collection_file: Collection file with documents
        """
        raw_docs = []
        with open(collection_file, 'r') as fin:
            while not self.end_of_file:
                raw_doc = self.read_doc(fin)
                if raw_doc:
                    raw_docs.append(raw_doc)

                # When the number of docs read in reaches our "memory limit" parameter
                # write out the docs to records
                if len(raw_docs) == CollectionProcessor.NUM_DOCS_TO_READ_IN_MEM:
                    self.normalize_docs(raw_docs)
                    raw_docs.clear()

        # Write any leftover docs to records
        self.normalize_docs(raw_docs)

    def read_doc(self, collection_file):
        """
        Read and return the next document from the collection file.

        :param collection_file: collection file handle
        :return document text
        """
        raw_text = ''
        html_end = f'</{self.html_tag}>'
        while html_end not in raw_text and not self.end_of_file:
            line = collection_file.readline()
            if line:
                raw_text += line
            else:
                self.end_of_file = True
                return None

        text = raw_text.strip().replace('\n', ' ')
        if not (text.startswith(f'<{self.html_tag} ') and text.endswith(html_end)):
            raise Exception(f'Error reading document: {text}')
        return text.split(html_end)[0]

    def normalize_docs(self, raw_docs):
        """
        Normalize a list of documents and call process_docs().

        :param raw_docs: List of raw documents.
        """
        with mp.Pool() as pool:
            docs = pool.map(normalize_doc, raw_docs)
        self.num_docs += len(docs)
        self.process_docs(docs)


def normalize_doc(raw_doc):
    """
    Normalize a single documents text.

    :param raw_doc: Document to process
    :return (int, list(str)): Document id and list of terms
    """
    right_brace_idx = raw_doc.find('>')
    doc_id = int(raw_doc[6:right_brace_idx])
    doc_text = raw_doc[right_brace_idx + 1:]
    raw_terms = doc_text.split(' ')

    terms = []
    for raw_term in raw_terms:
        term = normalize_term(raw_term, False)
        if term:
            terms.append(term)

    return doc_id, terms


def normalize_term(raw_term, stem):
    """
    Normalize a single term.

    :param raw_term: Term to process
    :param stem: Stem term
    :return str: Processed term
    """
    term = raw_term.lower()  # convert term to lowercase
    term = re.sub(r'[^a-z]', '', term)  # remove all non-alphabet characters
    if stem:
        term = CollectionProcessor.STEMMER.stem(term)  # stem term
    return term


def get_file_info(file_path):
    """
    Get a files info.

    :param file_path: File path
    :return:
    """
    return {
        'file_path': os.path.realpath(file_path),
        'file_size_b': os.path.getsize(file_path)
    }


def print_dict(dictionary):
    """
    Print a dictionary.

    :param dictionary: dictionary to print
    """
    pprint.pprint(dictionary, indent=2, width=100, sort_dicts=False)
