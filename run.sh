#!/bin/bash

trap 'trap - SIGTERM && kill -- -$$' SIGINT SIGTERM EXIT


run_test()
{
    ./phrase_query_processor.py -c data/${1}/collection.txt -q data/${1}/queries.txt -d testing/${1} -R
    diff testing/${1}/standard/results.txt testing/${1}/nextword/results.txt
}

run_test animal
run_test cheese
run_test times
run_test fire10
run_test cord19_keywords
run_test cord19_questions