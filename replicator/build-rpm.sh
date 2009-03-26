#!/bin/sh

VERSION=1.1.1
RPMBUILDDIR=`rpm --eval "%{_topdir}"`
SOURCE_TARBALL=rn.tar.gz
DIR=lounge-replicator-$VERSION/

rm -f $SOURCE_TARBALL 
rm -rf $DIR

mkdir $DIR
cp replication_notifier.py $DIR
tar czf rn.tar.gz $DIR

rm -f $RPMBUILDDIR/SOURCES/$DIR || true
cp $SOURCE_TARBALL $RPMBUILDDIR/SOURCES
rpmbuild -ba --clean --rmsource lounge-replicator.spec
