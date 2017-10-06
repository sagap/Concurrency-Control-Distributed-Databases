#!/bin/sh

BASEDIR=$(dirname $0)

cd $BASEDIR
rm -rf ./classes
mkdir -p ./classes
javac -d ./classes MVTO.java MVTOTest.java

for TEST in {1..5}
do
	`java -cp ./classes MVTOTest $TEST > /dev/null 2>&1`
	rc=$?
	if [[ $rc != 0 ]] ; then
	    echo "TEST $TEST: FAILED"
	fi
done