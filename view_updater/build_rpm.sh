#!/bin/sh
VER="1.0"
FILES="view_updater.py view_updater.logrotate view_updater.cron view_updater.spec"

topdir=`rpm --eval "%_topdir"`
tarfile=lounge-view-updater-$VER.tar.gz

tar cvzf lounge-view-updater-$VER.tar.gz $FILES
mv $tarfile $topdir/SOURCES
rpmbuild -ta $topdir/SOURCES/$tarfile
cp $topdir/RPMS/x86_64/lounge-view-updater-$VER-* .
