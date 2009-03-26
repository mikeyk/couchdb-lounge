Summary: Lounge Dumb Proxy
Name: lounge-dumb-proxy
Version: 1.0
Release: 1.%{?dist}
License: None
Group: Lounge

%description
A modified version of NGINX that handles sharding and failover of a couch cluster.

%prep
tar xvzf ../nginx_src/nginx-0.7.22.tar.gz
if [ $? -ne 0 ]; then
  exit $?
fi
cd nginx-0.7.22

%install
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
make install
if [ $? -ne 0 ]; then
  exit $?
fi
cd ..

install -m644 ../conf/nginx.conf /var/lounge/etc/nginx/nginx.conf
install -m644 ../conf/shards.conf /var/lounge/etc/shards.conf
install -m755 ../init.d/dumbproxy /etc/init.d/dumbproxy

%clean 

%files
%defattr(-,lounge,lounge)

/var/lounge/etc/nginx/
/var/lounge/nginx/
/var/lounge/log/nginx/
/var/lounge/sbin/nginx
/etc/init.d/dumbproxy
