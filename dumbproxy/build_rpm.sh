#!/bin/sh
mkdir -p build
rpmbuild --define="_builddir build" --define="_rpmdir rpms" -v -bb dumbproxy.spec
rm -Rf build
