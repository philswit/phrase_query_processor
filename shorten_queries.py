#!/usr/bin/env python3
"""shorten_queries.py - Shorten queries in a query file."""
import argparse
from utilities import CollectionProcessor


def main():
    """Shorten queries in a query file."""
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', type=str, required=True,
                        help='/path/to/query/input/file')
    parser.add_argument('-o', '--output', type=str, required=True,
                        help='/path/to/query/input/file')
    args = parser.parse_args()
    query_shortener = QueryShortener(args)
    query_shortener.read_queries()
    query_shortener.shorten_queries()


class QueryShortener(CollectionProcessor):
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
        self.read_docs(self.args.input)

    def process_docs(self, docs):
        """
        Call from the super class, process available queries.

        :param docs: Available queries
        """
        self.queries += docs

    def shorten_queries(self):
        all_terms = []
        for _, terms in self.queries:
            all_terms.extend(terms)
        idx = 0
        new_queries = []
        num_terms = 2
        while idx < len(all_terms):
            num_terms += 1
            if num_terms == 6:
                num_terms = 2
            new_query = []
            for _ in range(num_terms):
                if idx >= len(all_terms):
                    break
                new_query.append(all_terms[idx])
                idx += 1
            new_queries.append(' '.join(new_query))
        with open(self.args.output, 'w') as fout:
            for idx, new_query in enumerate(new_queries):
                fout.write(f'<Q ID={idx}>\n')
                fout.write(f'{new_query}\n')
                fout.write(f'</Q>\n\n')


if __name__ == '__main__':
    main()
