Summary: CouchDB replicator for the lounge
Name: lounge-replicator
Version: 1.2.0
Release: 4%{?dist}
URL: http://tilgovi.github.com/couchdb-lounge
License: GPL
Group: Applications/Databases
Requires: couchdb
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
Source: rn.tar.gz

%description
Replicator for CouchDb

%prep
%setup -q

%build

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/var/lib/lounge
install replication_notifier.py %{buildroot}/var/lib/lounge/replication_notifier.py

%clean
rm -rf %{buildroot}

%pre
mkdir -p /var/lib/lounge
mkdir -p /var/log/lounge/replicator
chown couchdb:couchdb /var/log/lounge/replicator

%preun
rm -f /var/log/lounge/replicator/*
rmdir /var/log/lounge/replicator

%files
%defattr(-,root,root,-)
/var/lib/lounge/replication_notifier.py
