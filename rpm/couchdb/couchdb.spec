%define tarname apache-couchdb
%define couchdb_user couchdb
%define couchdb_group couchdb
%define couchdb_home %{_localstatedir}/lib/couchdb
Name:           couchdb
Version:        0.10.0
Release:        1%{?dist}
Summary:        A document database server, accessible via a RESTful JSON API

Group:          Applications/Databases
License:        ASL 2.0
URL:            http://couchdb.apache.org/
Source0:        http://www.apache.org/dist/%{name}/%{version}/%{tarname}-%{version}.tar.gz
Source1:        %{name}.init
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:  erlang
BuildRequires:  libicu-devel 
BuildRequires:  js-devel 
BuildRequires:  help2man
#BuildRequires:  libcurl-devel
BuildRequires:  curl-devel

Requires:       erlang 
#Requires:       %{_bindir}/icu-config
Requires:       libicu-devel

#Initscripts
Requires(post): chkconfig
Requires(preun): chkconfig initscripts

# Users and groups
Requires(pre): shadow-utils

# Design-only replication patch
Patch0: designonly_replication-%{version}.patch

%description
Apache CouchDB is a distributed, fault-tolerant and schema-free 
document-oriented database accessible via a RESTful HTTP/JSON API. 
Among other features, it provides robust, incremental replication 
with bi-directional conflict detection and resolution, and is 
queryable and indexable using a table-oriented view engine with 
JavaScript acting as the default view definition language.

%prep
%setup -q -n %{tarname}-%{version}
# Patch pid location
#sed -i 's/%localstatedir%\/run\/couchdb.pid/%localstatedir%\/run\/couchdb\/couchdb.pid/g' \
#bin/couchdb.tpl.in

%patch0

%build
%configure
make %{?_smp_mflags}


%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT

## Install couchdb initscript
install -D -m 755 %{SOURCE1} $RPM_BUILD_ROOT%{_initrddir}/%{name}

# Create /var/log/couchdb
mkdir -p $RPM_BUILD_ROOT%{_localstatedir}/log/couchdb

# Create /var/run/couchdb
mkdir -p $RPM_BUILD_ROOT%{_localstatedir}/run/couchdb

# Create /var/lib/couchdb
mkdir -p $RPM_BUILD_ROOT%{_localstatedir}/lib/couchdb

# Create /etc/couchdb/default.d
mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/couchdb/default.d

# Create /etc/couchdb/local.d
mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/couchdb/local.d

## Use /etc/sysconfig instead of /etc/default
mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/sysconfig
mv $RPM_BUILD_ROOT%{_sysconfdir}/default/couchdb \
$RPM_BUILD_ROOT%{_sysconfdir}/sysconfig/couchdb
rm -rf $RPM_BUILD_ROOT%{_sysconfdir}/default

# Remove unecessary files
rm $RPM_BUILD_ROOT%{_sysconfdir}/rc.d/couchdb
rm -rf  $RPM_BUILD_ROOT%{_datadir}/doc/couchdb

# clean-up .la archives
find $RPM_BUILD_ROOT -name '*.la' -exec rm -f {} ';'


%clean
rm -rf $RPM_BUILD_ROOT


%pre
getent group %{couchdb_group} >/dev/null || groupadd -r %{couchdb_group}
getent passwd %{couchdb_user} >/dev/null || \
useradd -r -g %{couchdb_group} -d %{couchdb_home} -s /bin/bash \
-c "Couchdb Database Server" %{couchdb_user}
exit 0


%post
/sbin/ldconfig
/sbin/chkconfig --add couchdb


%postun -p /sbin/ldconfig


%preun
if [ $1 = 0 ] ; then
    /sbin/service couchdb stop >/dev/null 2>&1
    /sbin/chkconfig --del couchdb
fi


%files
%defattr(-,root,root,-)
%doc AUTHORS BUGS CHANGES LICENSE NEWS NOTICE README THANKS
%dir %{_sysconfdir}/couchdb
%dir %{_sysconfdir}/couchdb/local.d
%dir %{_sysconfdir}/couchdb/default.d
%config(noreplace) %attr(0644, %{couchdb_user}, root) %{_sysconfdir}/couchdb/default.ini
%config(noreplace) %attr(0644, %{couchdb_user}, root) %{_sysconfdir}/couchdb/local.ini
#%config(noreplace) %{_sysconfdir}/default/couchdb
%config(noreplace) %{_sysconfdir}/sysconfig/couchdb
%config(noreplace) %{_sysconfdir}/logrotate.d/couchdb
#%config %{_sysconfdir}/rc.d/couchdb
%{_initrddir}/couchdb
%{_bindir}/*
%{_libdir}/couchdb
%{_datadir}/couchdb
%{_mandir}/man1/*
%dir %attr(0755, %{couchdb_user}, root) %{_localstatedir}/log/couchdb
%dir %attr(0755, %{couchdb_user}, root) %{_localstatedir}/run/couchdb
%dir %attr(0755, %{couchdb_user}, root) %{_localstatedir}/lib/couchdb

%changelog
* Thu Oct 15 2009 Allisson Azevedo <allisson@gmail.com> 0.10.0-1
- Update to 0.10.0.

* Sun Oct 04 2009 Rahul Sundaram <sundaram@fedoraproject.org> 0.9.1-2
- Change url. Fixes rhbz#525949

* Thu Jul 30 2009 Allisson Azevedo <allisson@gmail.com> 0.9.1-1
- Update to 0.9.1.
- Drop couchdb-0.9.0-pid.patch.

* Tue Apr 21 2009 Allisson Azevedo <allisson@gmail.com> 0.9.0-2
- Fix permission for ini files.
- Fix couchdb.init start process.

* Tue Apr 21 2009 Allisson Azevedo <allisson@gmail.com> 0.9.0-1
- Update to 0.9.0.

* Tue Nov 25 2008 Allisson Azevedo <allisson@gmail.com> 0.8.1-4
- Use /etc/sysconfig for settings.

* Tue Nov 25 2008 Allisson Azevedo <allisson@gmail.com> 0.8.1-3
- Fix couchdb_home.
- Added libicu-devel for requires.

* Tue Nov 25 2008 Allisson Azevedo <allisson@gmail.com> 0.8.1-2
- Fix spec issues.

* Tue Nov 25 2008 Allisson Azevedo <allisson@gmail.com> 0.8.1-1
- Initial RPM release
