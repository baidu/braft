Name:           scsi-target-utils
Version:        1.0.24
Release:        2%{?dist}
Summary:        The SCSI target daemon and utility programs
Packager:       Roi Dayan <roid@mellanox.com>
Group:          System Environment/Daemons
License:        GPLv2
URL:            http://stgt.sourceforge.net/
Source0:        %{name}-%{version}-%{release}.tgz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildRequires:  pkgconfig libibverbs-devel librdmacm-devel libxslt docbook-style-xsl libaio-devel
%if %{defined suse_version}
Requires: aaa_base
%else
Requires(post): chkconfig
Requires(preun): chkconfig
Requires(preun): initscripts
%endif
Requires: lsof sg3_utils
ExcludeArch:    s390 s390x

%description
The SCSI target package contains the daemon and tools to setup a SCSI targets.
Currently, software iSCSI targets are supported.


%prep
%setup -q -n %{name}-%{version}-%{release}


%build
%{__make} %{?_smp_mflags} ISCSI_RDMA=1


%install
%{__rm} -rf %{buildroot}
%{__install} -d %{buildroot}%{_sbindir}
%{__install} -d %{buildroot}%{_mandir}/man5
%{__install} -d %{buildroot}%{_mandir}/man8
%{__install} -d %{buildroot}%{_initrddir}
%{__install} -d %{buildroot}/etc/bash_completion.d
%{__install} -d %{buildroot}/etc/tgt

%{__install} -p -m 0755 scripts/tgt-setup-lun %{buildroot}%{_sbindir}
%{__install} -p -m 0755 scripts/initd.sample %{buildroot}%{_initrddir}/tgtd
%{__install} -p -m 0755 scripts/tgt-admin %{buildroot}/%{_sbindir}/tgt-admin
%{__install} -p -m 0644 scripts/tgt.bashcomp.sh %{buildroot}/etc/bash_completion.d/tgt
%{__install} -p -m 0644 doc/manpages/targets.conf.5 %{buildroot}/%{_mandir}/man5
%{__install} -p -m 0644 doc/manpages/tgtadm.8 %{buildroot}/%{_mandir}/man8
%{__install} -p -m 0644 doc/manpages/tgt-admin.8 %{buildroot}/%{_mandir}/man8
%{__install} -p -m 0644 doc/manpages/tgt-setup-lun.8 %{buildroot}/%{_mandir}/man8
%{__install} -p -m 0644 doc/manpages/tgtimg.8 %{buildroot}/%{_mandir}/man8
%{__install} -p -m 0600 conf/targets.conf %{buildroot}/etc/tgt

pushd usr
%{__make} install DESTDIR=%{buildroot} sbindir=%{_sbindir}


%post
/sbin/chkconfig --add tgtd

%postun
if [ "$1" = "1" ] ; then
     /sbin/service tgtd condrestart > /dev/null 2>&1 || :
fi

%preun
if [ "$1" = "0" ] ; then
     /sbin/chkconfig tgtd stop > /dev/null 2>&1
     /sbin/chkconfig --del tgtd
fi


%clean
%{__rm} -rf %{buildroot}


%files
%defattr(-, root, root, -)
%doc README doc/README.iscsi doc/README.iser doc/README.lu_configuration doc/README.mmc
%{_sbindir}/tgtd
%{_sbindir}/tgtadm
%{_sbindir}/tgt-setup-lun
%{_sbindir}/tgt-admin
%{_sbindir}/tgtimg
%{_mandir}/man5/*
%{_mandir}/man8/*
%{_initrddir}/tgtd
/etc/bash_completion.d/tgt
%attr(0600,root,root) %config(noreplace) /etc/tgt/targets.conf
