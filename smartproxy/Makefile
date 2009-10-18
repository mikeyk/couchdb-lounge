# build number, increment with each new build, reset on new version
RELEASE = "0"


PY_VERSION := $(shell python -V 2>&1)
PY_EXTRA = --install-script install.sh
ifeq ($(PY_VERSION), Python 2.3.4)
	PY_EXTRA=
endif

default:
	echo $(PY_VERSION)
	python setup.py bdist_rpm --release="$(RELEASE)" --requires "Twisted, python-cjson, zope.interface, python-lounge, python-simplejson" $(PY_EXTRA)
clean:
	rm -f MANIFEST
	rm -rf dist build
	find . -name "*.pyc" -exec rm -f {} \;
	find . -name "*~" -exec rm -f {} \;
	find . -name "#*#" -exec rm -f {} \;
