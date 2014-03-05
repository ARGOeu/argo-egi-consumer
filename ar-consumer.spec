%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}
%define pylib %{python_sitelib}/arconsumer

Name: ar-consumer
Summary: A/R Comp Engine message consumer
Version: 1.1.0
Release: 1%{?dist}
License: ASL 2.0
Buildroot: %{_tmppath}/%{name}-buildroot
Group:     EGI/SA4
BuildArch: noarch
Source0:   %{name}-%{version}.tar.gz
Requires: stomppy >= 3.1.6

%description
Installs the service for consuming SAM monitoring results
from the EGI message broker infrastructure.

%prep
%setup 

%install 
%{__rm} -rf %{buildroot}
install --directory %{buildroot}/etc/init.d
install --directory %{buildroot}/usr/bin
install --directory %{buildroot}/etc/ar-consumer/
install --directory %{buildroot}/%{python_sitelib}
install --directory %{buildroot}/var/lib/ar-consumer
install --directory %{buildroot}/var/log/ar-consumer
install --mode 644 etc/ar-consumer/ar-consumer.conf %{buildroot}/etc/ar-consumer/
install --mode 644 etc/ar-consumer/messagewritter.conf %{buildroot}/etc/ar-consumer/
install --mode 755 init.d/ar-consumer %{buildroot}/etc/init.d
install --mode 755 bin/ar-consumer %{buildroot}/usr/bin
%{__cp} -rpf arconsumer %{buildroot}/%{python_sitelib}/
%{__python} setup.py install_lib -O1 --skip-build --build-dir=arconsumer --install-dir=%{buildroot}%{pylib}

%clean
%{__rm} -rf %{buildroot}

%files
%defattr(0644,root,root)
%attr(0755,root,root) /usr/bin/ar-consumer
%attr(0755,root,root) /etc/init.d/ar-consumer
%config(noreplace) /etc/ar-consumer/ar-consumer.conf
%config(noreplace) /etc/ar-consumer/messagewritter.conf
%attr(0750,arstats,arstats) /var/lib/ar-consumer
%attr(0750,arstats,arstats) /var/log/ar-consumer
%{pylib}

%post
/sbin/chkconfig --add ar-consumer

%pre
getent group arstats > /dev/null || groupadd -r arstats
getent passwd arstats > /dev/null || \
    useradd -r -g arstats -d /var/lib/ar-consumer -s /sbin/nologin \
    -c "AR Comp Engine user" arstats

%preun
if [ "$1" = 0 ] ; then
   /sbin/service ar-consumer stop
   /sbin/chkconfig --del ar-consumer
fi

%changelog
* Mon Nov 4 2013 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.0.1-2%{?dist}
- Fixes for consumer
* Thu Oct 3 2013 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.0.1-1%{?dist}
- Updates and fixes for consumer
* Thu Aug 1 2013 Emir Imamagic <eimamagi@srce.hr> - 1.0.0-1%{?dist}
- Initial release
