# build number, increment with each new build, reset on new version
RELEASE = 1`rpm --eval %{?dist}`


PY_VERSION := $(shell python -V 2>&1)
PY_EXTRA = --install-script install.sh
ifeq ($(PY_VERSION), Python 2.3.4)
	PY_EXTRA=
endif

ifeq ($(strip $(DESTDIR)),)
	root=
else
	root=--root $(DESTDIR)
endif

default:
	python setup.py build

install:
	python setup.py install $(root)

rpm:
	echo $(PY_VERSION)
	python setup.py bdist_rpm --release="$(RELEASE)" --requires "Twisted, python-pyicu, python-cjson, zope.interface, python-lounge >= 1.3.3" $(PY_EXTRA)

clean:
	rm -f MANIFEST
	rm -rf dist build
	find . -name "*.pyc" -exec rm -f {} \;
	find . -name "*~" -exec rm -f {} \;
	find . -name "#*#" -exec rm -f {} \;
