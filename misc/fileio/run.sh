#!/bin/bash

# compile Go program.
go install
PRG=fileio
BUFSIZE=1048576

# Small 1GB file. Choose files size << RAM size.
FSIZE=1000000000
FNAME=/tmp/1GB.data

echo ""
echo "Writing 1GB file: $FNAME"
$PRG --write  --fname=$FNAME --rsize=$BUFSIZE --fsize=$FSIZE

echo ""
echo "Sequntially scanning (os.File.Read) 1GB file: $FNAME"
$PRG --read    --fname=$FNAME --rsize=$BUFSIZE
$PRG --read    --fname=$FNAME --rsize=$BUFSIZE

echo ""
echo "Random reads with different record sizes:"
for size in 1 10 100 1000 10000 100000 1000000
do
  $PRG --pread --rsize=$size --n=1000000 --fname=$FNAME
  $PRG --mmap  --rsize=$size --n=1000000 --fname=$FNAME
  echo ""
done

echo "Sequantially scanning (mmap + copy) 1GB file: $FNAME"
$PRG --mread    --fname=$FNAME --rsize=$BUFSIZE
$PRG --mread    --fname=$FNAME --rsize=$BUFSIZE

echo ""
echo "Random reads with different record sizes:"
for size in 1 10 100 1000 10000 100000 1000000
do
  $PRG --pread --rsize=$size --n=1000000 --fname=$FNAME
  $PRG --mmap  --rsize=$size --n=1000000 --fname=$FNAME
  echo ""
done

# Large 32GB file. Choose files size 2x of RAM size.
FSIZE=32000000000
FNAME=/tmp/32GB.data

echo ""
echo "Writing LARGE 32GB file: $FNAME"
$PRG --write  --fname=$FNAME --rsize=$BUFSIZE --fsize=$FSIZE

echo ""
echo "Sequntially scanning (os.File.Read) 32GB file: $FNAME"
$PRG --read    --fname=$FNAME --rsize=$BUFSIZE
echo "Sequantially scanning (mmap + copy) 32GB file: $FNAME"
$PRG --mread    --fname=$FNAME --rsize=$BUFSIZE

echo ""
echo "Random reads with different record sizes:"
for size in 1 10 100 1000 10000 100000 1000000
do
  $PRG --pread --rsize=$size --n=1000000 --fname=$FNAME
  $PRG --mmap  --rsize=$size --n=1000000 --fname=$FNAME
  echo ""
done
