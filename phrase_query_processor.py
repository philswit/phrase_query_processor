#!/usr/bin/env python3
"""phrase_query_processor.py - Process a collection and queries and produce document matches."""
import argparse
import cProfile
import datetime
import os
import time
import pyprof2calltree

from build_record_file import build_record_file
from build_index import build_index
from process_queries import process_queries
from utilities import print_dict


def main():
    """
    Process a collection file and query file and produce document matches.

    The following steps are completed:
        1. Process a collection file and build an index (If the index doesnt exist or using -R)
        2. Process a query file and output a results file
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--collection-file', type=str, required=True,
                        help='/path/to/collection/input/file')
    parser.add_argument('-q', '--query-file', type=str, required=True,
                        help='/path/to/query/input/file')
    parser.add_argument('-d', '--output-dir', type=str, required=True,
                        help='/path/to/output/directory')
    parser.add_argument('-s', '--stem', action='store_true', help='Enable stemming')
    parser.add_argument('-R', '--rebuild-index', action='store_true',
                        help='Rebuild index if it already exists')
    parser.add_argument('-p', '--profile', action='store_true', help='Enable profiling')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    run_processor(parser)


def run_processor(parser):
    """
    Execute the program.

    :param parser: Program argument parser
    """
    # Run through profiler if -p is given
    metadata_list = []
    for nextword in [False, True]:
        args = Arguments(parser, nextword)
        c_profile = cProfile.Profile()
        if args.profile:
            print('Profiling enabled')
            c_profile.enable()
            metadata = run_single_processor(args)
            c_profile.disable()
            now = datetime.datetime.now()
            profile_file = f'profile.out.{now.hour}.{now.minute}.{now.second}'
            pyprof2calltree.convert(c_profile.getstats(), profile_file)
        else:
            metadata = run_single_processor(args)
        metadata_list.append(metadata)

    standard_metadata = metadata_list[0]
    print('\nStandard index metadata:')
    print_dict(standard_metadata)

    print('\nNextword index metadata:')
    nextword_metadata = metadata_list[1]

    print_dict(nextword_metadata)
    build_metrics_csv(args.metrics_file, standard_metadata, nextword_metadata)


def run_single_processor(args):
    """
    Execute standard or nextword processor.

    :param args: Program arguments
    """
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    # build index if necessary
    build_index_runtime = 0.0
    for fpath in [args.record_file, args.inverted_file, args.lexicon_file]:
        if not os.path.exists(fpath):
            args.rebuild_index = True
    if args.rebuild_index:
        build_index_runtime = build_record_file_and_index(args)

    # process the queries
    metadata = process_queries(args)

    elapsed = build_index_runtime + metadata['process_queries_runtime']
    metadata['build_index_runtime'] = build_index_runtime
    metadata['total_runtime'] = elapsed
    metadata['test_name'] = args.test_name

    return metadata


def build_record_file_and_index(args):
    """
    Build the temporary record file and index.

    :param args: Program arguments
    """
    print('Building index')
    start_time = time.time()

    build_record_file(args)
    build_index(args)

    elapsed = time.time() - start_time
    print(f'Built index, time elapsed: {elapsed:.2f} seconds')
    return elapsed


def build_metrics_csv(metrics_file, standard_metadata, nextword_metadata):

    num_queries = standard_metadata['num_queries']
    num_matched_queries = standard_metadata['num_matched_queries']
    num_matched_queries_nw = nextword_metadata['num_matched_queries']

    inverted_size = standard_metadata['inverted_file']['file_size_b']
    inverted_size_nw = nextword_metadata['inverted_file']['file_size_b']

    lexicon_size = standard_metadata['lexicon_file']['file_size_b']
    lexicon_size_nw = nextword_metadata['lexicon_file']['file_size_b']

    index_size = inverted_size + lexicon_size
    index_size_nw = inverted_size_nw + lexicon_size_nw

    index_runtime = standard_metadata['build_index_runtime']
    index_runtime_nw = nextword_metadata['build_index_runtime']

    query_runtime = standard_metadata['process_queries_runtime']
    query_runtime_nw = nextword_metadata['process_queries_runtime']

    total_runtime = query_runtime + index_runtime
    total_runtime_nw = query_runtime_nw + index_runtime_nw

    lines = [
        ['test_name', standard_metadata['test_name']],
        ['number_of_docs', standard_metadata['number_of_docs']],
        ['collection_size', standard_metadata['collection_size']],
        ['vocabulary_size', standard_metadata['vocabulary_size']],
        ['number_of_queries', num_queries],
        ['mean_query_length', standard_metadata['mean_query_length']],

        ['number_of_matched_queries', num_matched_queries],
        ['number_of_matched_queries_nw', num_matched_queries_nw],
        ['percent_queries_matched', (float(num_matched_queries_nw) / num_queries) * 100.0],

        ['inverted_size', inverted_size],
        ['inverted_size_nw', inverted_size_nw],
        ['inverted_size_change', (inverted_size_nw / inverted_size) * 100.0],

        ['lexicon_size', lexicon_size],
        ['lexicon_size_nw', lexicon_size_nw],
        ['inverted_size_change', (lexicon_size_nw / lexicon_size) * 100.0],

        ['index_size', index_size],
        ['index_size_nw', index_size_nw],
        ['inverted_size_change', (index_size_nw / index_size) * 100.0],

        ['index_runtime', index_runtime],
        ['index_runtime_nw', index_runtime_nw],
        ['index_runtime_change', (index_runtime_nw / index_runtime) * 100.0],

        ['total_query_runtime', query_runtime],
        ['total_query_runtime_nw', query_runtime_nw],
        ['mean_query_runtime', query_runtime / num_queries],
        ['mean_query_runtime_nw', query_runtime_nw / num_queries],
        ['query_runtime_change', (query_runtime_nw / query_runtime) * 100.0],

        ['total_runtime', total_runtime],
        ['total_runtime_nw', total_runtime_nw],
        ['total_runtime_change', (total_runtime_nw / total_runtime) * 100.0],
    ]
    with open(metrics_file, 'w') as fout:
        for line in lines:
            fout.write(f'{line[0]},{line[1]}\n')


class Arguments:
    """The Arguments class sets command line arguments and some additional file paths."""

    def __init__(self, parser, nextword):
        """
        Initialize am Arguments instance.

        :param parser: ArgumentParser with command line arguments.
        """
        args = parser.parse_args()
        self.collection_file = args.collection_file
        self.query_file = args.query_file
        subdir = 'standard'
        if nextword:
            subdir = 'nextword'
        self.output_dir = os.path.join(args.output_dir, subdir)
        self.stem = args.stem
        self.rebuild_index = args.rebuild_index
        self.profile = args.profile
        self.verbose = args.verbose
        self.nextword = nextword
        self.test_name = os.path.basename(args.output_dir)
        self.record_file = os.path.join(self.output_dir, 'record.txt')
        self.lexicon_file = os.path.join(self.output_dir, 'lexicon.bin')
        self.inverted_file = os.path.join(self.output_dir, 'inverted.bin')
        self.query_results_file = os.path.join(self.output_dir, 'results.txt')
        self.metrics_file = os.path.join(args.output_dir, 'metrics.csv')


if __name__ == '__main__':
    main()
