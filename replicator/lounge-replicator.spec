Summary: CouchDB replicator for the lounge
Name: lounge-replicator
Version: 1.2.0
Release: 10%{?dist}
URL: http://tilgovi.github.com/couchdb-lounge
License: GPL
Group: Applications/Databases
Requires: couchdb
Requires: python-curl
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
mkdir -p %{buildroot}/var/log/lounge/replicator
install replication_notifier.py %{buildroot}/var/lib/lounge/replication_notifier.py

%clean
rm -rf %{buildroot}

%pre
mkdir -p /var/lib/lounge

%files
%defattr(-,root,root,-)
/var/lib/lounge/replication_notifier.py
%dir %attr(0755,couchdb,couchdb)/var/log/lounge/replicator
