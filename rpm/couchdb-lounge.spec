Name:		couchdb-lounge
Version: 	1.1	
Release:	1%{?dist}
Summary:	Clustered CouchDB
Group: 		Database/CouchDBCluster
License: 	Apache

BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

Requires:  lounge-dumb-proxy, lounge-smartproxy, couchdb, lounge-replicator, python-lounge

%description
Metapackage wrapping the dependencies for the various lounge components

%prep

%build

%install

%post
/etc/init.d/couchdb stop
/etc/init.d/couchdb start
/etc/init.d/smartproxyd restart
/etc/init.d/dumbproxy restart

