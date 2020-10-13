#!/bin/sh

#
# basic map-reduce test
#

./mrsequential ../mrapps/nocrash.so ./pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*