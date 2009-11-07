Summary: Lounge Dumb Proxy
Name: lounge-dumb-proxy
Version: 1.0
Release: 0
URL: http://tilgovi.github.com/couchdb-lounge
License: None
Group: Lounge
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description
A modified version of NGINX that handles sharding and failover of a couch cluster.

%prep
tar xvzf ../nginx_src/nginx-0.7.22.tar.gz
if [ $? -ne 0 ]; then
  exit $?
fi
cd nginx-0.7.22

%build
cd nginx-0.7.22
MODULES="--add-module=../../nginx_lounge_module"
CFLAGS="--with-cc-opt=`pkg-config --cflags json`"
LIBS="--with-ld-opt=`pkg-config --libs-only-L json`"
PREFIX="--prefix=/var/lounge/nginx"
SBIN="--sbin-path=/var/lounge/sbin/nginx"
CONF="--conf-path=/var/lounge/etc/nginx/nginx.conf"
ACCESS_LOG="--http-log-path=/var/lounge/log/nginx/access.log"
ERROR_LOG="--error-log-path=/var/lounge/log/nginx/error.log"
PID="--pid-path=/var/lounge/nginx/pid/nginx.pid"
LOCK="--lock-path=/var/lounge/nginx/lock/nginx.lock"
./configure $PREFIX $SBIN $CONF $ACCESS_LOG $ERROR_LOG $PID $LOCK $MODULES $CFLAGS $LIBS
if [ $? -ne 0 ]; then
  exit $?
fi
make
if [ $? -ne 0 ]; then
  exit $?
fi
cd ..

%install
cd nginx-0.7.22
make DESTDIR=$RPM_BUILD_ROOT install
if [ $? -ne 0 ]; then
  exit $?
fi
cd ../..

echo `pwd`

install -d $RPM_BUILD_ROOT/var/lounge/etc/nginx
install -d $RPM_BUILD_ROOT/etc/init.d
install -m644 conf/nginx.conf  $RPM_BUILD_ROOT/var/lounge/etc/nginx/nginx.conf
install -m644 conf/shards.conf $RPM_BUILD_ROOT/var/lounge/etc/shards.conf
install -m755 init.d/dumbproxy $RPM_BUILD_ROOT/etc/init.d/dumbproxy

%clean 
rm -Rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)

/var/lounge/etc/nginx/
/var/lounge/nginx/
/var/lounge/log/nginx/
/var/lounge/sbin/nginx
/etc/init.d/dumbproxy
/var/lounge/etc/shards.conf
