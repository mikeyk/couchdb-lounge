Summary: A document database server, accessible via a RESTful JSON API
Name: couchdb
Version: 0.10.0
Release: 1
URL: http://www.couchdb.com
License: GPL
Group: Applications/Databases
Source: apache-couchdb-%{version}.tar.gz
Source1: couchdb_meebo_default
Requires(pre): /usr/sbin/useradd
Requires: erlang icu libicu-devel
Requires: meebo-lounge-replicator python-lounge
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildRequires: autoconf
BuildRequires: automake
BuildRequires: libicu-devel

Patch0: designonly_replication-%(version).patch

%description
Couchdb is a document database server, accessible via a RESTful JSON API. It has
an ad-hoc and schema-free with a flat address space. CouchDB is distributed,
featuring robust, incremental replication with bi-directional conflict detection
and management. It features a table oriented reporting engine that uses
Javascript as a query language.

%prep
%setup -n apache-couchdb-%{version} -q
%patch0

%build
%configure --with-erlang=%{_libdir}/erlang/usr/include --with-js-include=/usr/include/xulrunner-sdk-1.9/js --with-js-lib=/usr/lib64/xulrunner-sdk-1.9/sdk/lib
make DESTDIR=%{buildroot}

%install
rm -rf %{buildroot}
make DESTDIR=%{buildroot} install

mkdir -p %{buildroot}/etc/default
install %{SOURCE1} %{buildroot}/etc/default/couchdb

%clean
rm -rf %{buildroot}

%pre
/usr/sbin/useradd -s /bin/bash -m -r -d /home/couchdb \
    -c "couchdb user" couchdb &>/dev/null || :
mkdir -p /var/log/couchdb
chown couchdb:couchdb /var/log/couchdb
mkdir -p /var/lib/couchdb
chown couchdb:couchdb /var/lib/couchdb
touch /var/run/couchdb.pid
chown couchdb:couchdb /var/run/couchdb.pid

%files
%defattr(-,root,root,-)

%define couchdb_version_dir couch-%{version}
/etc/couchdb
/etc/default/couchdb
/etc/logrotate.d/couchdb
/etc/rc.d/couchdb
%{_libdir}/couchdb
%{_bindir}/couchdb
%{_bindir}/couchjs
/usr/share/couchdb
/usr/share/doc/couchdb
%changelog
* Thu Oct 29 2009 Randall Leeds <randall.leeds@gmail.com>
Updated for CouchDB 0.10.0
* Thu Jan 17 2008 Ditesh Shashikant Gathani <ditesh@gathani.org> 
- Initial Release
