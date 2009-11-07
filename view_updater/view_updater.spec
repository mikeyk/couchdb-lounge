Summary: Finds all the views on a locally running instance of couchdb and retrieves them, forcing an index update
Name: lounge-view-updater
Version: 1.0
Release: 4%{?dist}
URL: http://tilgovi.github.com/couchdb-lounge
Group: Lounge/Utils
License: None
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%description
Finds and retrieves all views on a local couchdb instance

%prep
%setup -n lounge-view-updater -c

%install
install -D -m755 view_updater.py $RPM_BUILD_ROOT/var/lounge/bin/view_updater.py
install -D -m644 view_updater.logrotate $RPM_BUILD_ROOT/etc/logrotate.d/view_updater
install -D -m644 view_updater.cron $RPM_BUILD_ROOT/etc/cron.d/view_updater

%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(-,root,root,-)
/var/lounge/bin/view_updater.py
/etc/logrotate.d/view_updater
/etc/cron.d/view_updater
%doc





%changelog

