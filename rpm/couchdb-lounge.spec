Name:		couchdb-lounge
Version: 	1.2.0
Release:	4%{?dist}
Summary:	Clustered CouchDB
Group: 		Database/CouchDBCluster
License: 	Apache

BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

Requires:  lounge-dumb-proxy >= 1.2.0, lounge-smartproxy >= 1.2.2, couchdb >= 0.10.0, lounge-replicator >= 1.2.0

%description
Metapackage wrapping the dependencies for the various lounge components

%prep

%build

%clean

%install

%post
/etc/init.d/couchdb stop
/etc/init.d/couchdb start
/etc/init.d/smartproxyd restart
/etc/init.d/dumbproxy restart

%files
