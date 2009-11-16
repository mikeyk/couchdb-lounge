Name:		couchdb-lounge
Version: 	1.2.0
Release:	7%{?dist}
Summary:	Clustered CouchDB
Group: 		Database/CouchDBCluster
License: 	Apache

BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

Requires:  lounge-dumbproxy >= 1.2.0, lounge-smartproxy >= 1.2.2, couchdb >= 0.10.0, lounge-replicator >= 1.2.0

%description
Metapackage wrapping the dependencies for the various lounge components

%prep
cp -p %{_sourcedir}/lounge.ini .

%build

%clean

%install
mkdir -p %{buildroot}/etc/couchdb/local.d
cp %{_builddir}/lounge.ini %{buildroot}/etc/couchdb/local.d/lounge.ini

%post
/etc/init.d/couchdb stop
/etc/init.d/couchdb start
/etc/init.d/smartproxyd restart
/etc/init.d/dumbproxy restart

%files
%config(noreplace)/etc/couchdb/local.d/lounge.ini

