#!/bin/sh

rm -rf init.log || true
./dynamodb_init.sh > init.log

sed -i 's/REGRESS =.*/REGRESS = server_options connection_validation pushdown extra\/delete extra\/insert extra\/json extra\/jsonb extra\/select extra\/update /' Makefile

make clean
make
mkdir -p results/extra
make check $1| tee make_check.out
