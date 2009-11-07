Summary: CouchDB replicator for the lounge
Name: lounge-replicator
Version: 1.1.1
Release: 1
URL: http://tilgovi.github.com/couchdb-lounge
License: GPL
Group: Applications/Databases
Requires: couchdb
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
Source: rn.tar.gz

%description
Replicator for CouchDb

%prep
%setup -q

%build

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/var/lounge/lib/
install replication_notifier.py %{buildroot}/var/lounge/lib/replication_notifier.py

%clean
rm -rf %{buildroot}

%pre
mkdir -p /var/lounge/lib

%files
%defattr(-,root,root,-)

/var/lounge/lib/replication_notifier.py
