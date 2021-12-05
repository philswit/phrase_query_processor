#!/usr/bin/env python3
"""process_queries.py - Process a file containing queries."""
import os
import pickle
import time

from utilities import CollectionProcessor, get_file_info, INT_BYTES, print_dict

NUM_DOCS_TO_RANK = 100
VERBOSE = False


def process_queries(args):
    """
    Evaluate queries in a query file against an index.

    :param args: Program arguments
    """
    if args.verbose:
        global VERBOSE
        VERBOSE = True

    info(f'\nProcessing query file: {os.path.abspath(args.query_file)}')
    start_time = time.time()

    query_file_processor = QueryFileProcessor(args)
    queries = query_file_processor.read_queries()
    if args.nextword:
        query_processor = NextwordQueryProcessor(args, queries)
    else:
        query_processor = StandardQueryProcessor(args, queries)
    metadata = query_processor.lexicon['metadata']
    info_dict('\nIndex metadata:', metadata)
    debug_dict('\nIndex terms:', query_processor.lexicon['terms'])
    query_processor.process_queries()

    elapsed = time.time() - start_time
    info(f'\nProcessed queries, time elapsed: {elapsed:.2f} seconds\n')

    metadata['process_queries_runtime'] = elapsed
    metadata['num_queries'] = len(queries)
    metadata['mean_query_length'] = query_processor.collection_size / len(queries)
    metadata['mean_query_runtime'] = elapsed / len(queries)
    return query_processor.lexicon['metadata']


class QueryFileProcessor(CollectionProcessor):
    """The QueryFileProcessor will read a query file, normalize and return all queries."""

    def __init__(self, args):
        """
        Initialize the query file processor with program arguments.

        :param args: Program arguments
        """
        super().__init__(args, 'Q')
        self.queries = []

    def read_queries(self):
        """Read all queries in query file."""
        self.read_docs(self.args.query_file)
        return self.queries

    def process_docs(self, docs):
        """
        Call from the super class, process available queries.

        :param docs: Available queries
        """
        self.queries += docs


class QueryProcessor:
    """The query processor processes a list of queries and writes the results to disk."""

    def __init__(self, args, queries):
        """
        Initialize the query processor with program arguments and queries.

        :param args: Program arguments
        :param queries: List of queries to process
        """
        self.args = args
        self.collection_size = 0
        self.lexicon = pickle.load(open(self.args.lexicon_file, 'rb'))
        self.lexicon['metadata']['lexicon_file'] = get_file_info(self.args.lexicon_file)
        self.lexicon['metadata']['inverted_file'] = get_file_info(self.args.inverted_file)
        self.inverted_file = open(self.args.inverted_file, 'rb')
        self.queries = queries
        self.results = []

    def get_doc_matches(self, terms):
        raise NotImplementedError()

    def process_queries(self):
        """Process all queries and write results to disk."""
        for query in self.queries:
            self.process_query(query)
        with open(self.args.query_results_file, 'w') as fout:
            for query_id, doc_ids in self.results:
                doc_ids = list(doc_ids)
                doc_ids.sort()
                doc_ids = ','.join([f'P{doc_id}' for doc_id in doc_ids])
                fout.write(f'Q{query_id},{doc_ids}\n')
        self.lexicon['metadata']['num_matched_queries'] = len(self.results)

    def process_query(self, query):
        """
        Process a single query.

        :param query: Query tuple to process
        """
        query_id = query[0]
        terms = query[1]
        self.collection_size += len(terms)
        debug(f'Processing query: {query_id}: {terms}')

        doc_matches = self.get_doc_matches(terms)
        self.results.append([query_id, doc_matches])
        debug(f'Query was found in the following documents: {doc_matches}')

    def read_int(self):
        """Read an integer from the inverted file."""
        return int.from_bytes(self.inverted_file.read(INT_BYTES), byteorder='big', signed=False)

    def skip_int(self):
        """Skip an integer from the inverted file."""
        self.inverted_file.read(INT_BYTES)


class StandardQueryProcessor(QueryProcessor):

    def __init__(self, args, queries):
        super().__init__(args, queries)

    def get_doc_matches(self, terms):
        if not terms:
            return set()
        pos_dict = self.get_corpus_term_pos_list(terms[0])
        if len(terms) > 1:
            for term in terms[1:]:
                next_term_pos_dict = self.get_corpus_term_pos_list(term)
                for doc_id, pos_list_list in pos_dict.items():
                    for pos_list in pos_list_list:
                        last_pos = pos_list[-1]
                        if next_term_pos_dict.get(doc_id):
                            next_pos_list_list = next_term_pos_dict[doc_id]
                            for next_pos_list in next_pos_list_list:
                                for next_pos in next_pos_list:
                                    if next_pos == last_pos + 1:
                                        pos_list.append(next_pos)
        doc_matches = set()
        for doc_id, pos_list_list in pos_dict.items():
            for pos_list in pos_list_list:
                if len(pos_list) == len(terms):
                    doc_matches.add(doc_id)
                    continue
        return doc_matches

    def get_corpus_term_pos_list(self, term):
        if not self.lexicon['terms'].get(term):
            return {}
        term_dict = self.lexicon['terms'][term]

        self.inverted_file.seek(term_dict['offset'] * INT_BYTES)

        term_corpus_pos_dict = {}
        for _ in range(term_dict['doc_freq']):
            doc_id = self.read_int()
            term_corpus_pos_dict[doc_id] = []

            doc_tf = self.read_int()
            for _ in range(doc_tf):
                term_corpus_pos_dict[doc_id].append([self.read_int()])
        return term_corpus_pos_dict


class NextwordQueryProcessor(QueryProcessor):

    def __init__(self, args, queries):
        super().__init__(args, queries)
        self.ordered_unique_term_pairs = {}
        self.query_terms = []
        self.query_term_pairs = []
        self.covered = []
        self.query_results = {}

    def setup_term_pairs(self, terms):
        terms_dict = self.lexicon['terms']
        self.query_terms = terms
        self.ordered_unique_term_pairs = {}
        self.query_term_pairs = []
        for idx, first_term in enumerate(terms[:-1]):
            next_term = terms[idx + 1]
            term_pair = (first_term, next_term)
            if not terms_dict.get(first_term):
                return False
            if not terms_dict.get(next_term):
                return False
            self.ordered_unique_term_pairs[term_pair] = terms_dict[first_term]['collection_freq']
            self.query_term_pairs.append(term_pair)

        self.covered = [False] * len(terms)

        self.ordered_unique_term_pairs = sort_dict_by_value(self.ordered_unique_term_pairs, True)
        debug_dict("\nOrdered unique query term pairs", self.ordered_unique_term_pairs)
        debug_dict("\nQuery term pairs", self.query_term_pairs)
        return True

    def update_query_term_pairs(self, term_pair, postings_dict):
        self.query_results[term_pair] = postings_dict

    def get_doc_matches(self, terms):
        if not terms:
            return set()

        if len(terms) == 1:
            postings = self.find_postings((terms[0], None))
            print(postings)
            return set(postings.keys())

        if not self.setup_term_pairs(terms):
            return set()

        self.query_results = {}
        for term_pair in self.ordered_unique_term_pairs:
            postings_dict = self.find_postings(term_pair)

            # If any term pair is missing, no doc matches are found
            if not postings_dict:
                return set()

            self.update_query_term_pairs(term_pair, postings_dict)

            for idx, query_term_pair in enumerate(self.query_term_pairs):
                if term_pair == query_term_pair:
                    self.covered[idx] = True
                    self.covered[idx + 1] = True
            debug(f'Covered = {self.covered}')
            if False not in self.covered:
                debug(f'All terms covered')
                break

        debug_dict('Query results', self.query_results)

        return self.merge_query_results()

    def merge_query_results(self):
        single_term_results = {}
        for term_pair, postings in self.query_results.items():
            for doc_id, pos_list in postings.items():
                for idx, term in enumerate(list(term_pair)):
                    if term not in single_term_results:
                        single_term_results[term] = {}
                    term_results = single_term_results[term]
                    if doc_id not in term_results:
                        term_results[doc_id] = set()
                    for pos in pos_list:
                        term_results[doc_id].add(pos + idx)
        debug_dict("Single term results", single_term_results)

        first_term_results = single_term_results[self.query_terms[0]]
        results = {}
        for doc_id, pos_set in first_term_results.items():
            results[doc_id] = [[pos] for pos in pos_set]

        for term in self.query_terms[1:]:
            new_results = single_term_results[term]
            for doc_id, new_pos_set in new_results.items():
                if doc_id in results:
                    pos_list_list = results[doc_id]
                    for idx, pos_list in enumerate(pos_list_list):
                        next_term = pos_list[-1] + 1
                        if next_term in new_pos_set:
                            pos_list_list[idx].append(next_term)
        doc_matches = set()
        for doc_id, pos_list_list in results.items():
            for pos_list in pos_list_list:
                if len(pos_list) == len(self.query_terms):
                    doc_matches.add(doc_id)
        return doc_matches

    def find_postings(self, term_pair):
        first_term = term_pair[0]
        next_term = term_pair[1]
        term_dict = self.lexicon['terms']
        if not term_dict.get(first_term):
            return {}

        postings_dict = {}

        first_term_dict = self.lexicon['terms'][first_term]
        num_next_terms = first_term_dict['num_next_terms']
        offset = first_term_dict['offset']
        self.inverted_file.seek(offset * INT_BYTES)
        for _ in range(num_next_terms):
            next_term_id = self.read_int()
            this_next_term = self.lexicon['term_ids'][next_term_id]
            pair_doc_freq = self.read_int()
            if this_next_term == next_term or next_term is None:
                for _ in range(pair_doc_freq):
                    doc_id = self.read_int()
                    postings_dict[doc_id] = []
                    pos_list_len = self.read_int()
                    for _ in range(pos_list_len):
                        postings_dict[doc_id].append(self.read_int())
            else:
                for _ in range(pair_doc_freq):
                    self.skip_int()
                    pos_list_len = self.read_int()
                    self.inverted_file.seek(pos_list_len * INT_BYTES, 1)

        debug_dict(f'\nPostings found: {term_pair}', postings_dict)

        return postings_dict


def sort_dict_by_value(dictionary, reverse):
    """Sort a dictionary in reverse by values."""
    return dict(sorted(dictionary.items(), key=lambda item: item[1], reverse=reverse))


def info(msg):
    print(msg)


def debug(msg):
    if VERBOSE:
        print(msg)


def info_dict(msg, dictionary):
    print(msg)
    print_dict(dictionary)


def debug_dict(msg, dictionary):
    if VERBOSE:
        info_dict(msg, dictionary)
