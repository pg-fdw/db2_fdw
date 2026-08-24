%global _prefix /usr/local
Summary: PostgreSQL %{pgversion} db2 foreign data wrapper
Name: postgresql%{pgversion}-db2_fdw
Version: %{fdw_version}
Release: 1%{?dist}
License: PostgreSQL License
Group: Applications/Databases
Url: https://github.com/pg-fdw/db2_fdw
%undefine _disable_source_fetch
BuildArch: x86_64

BuildRequires: make
BuildRequires: gcc
BuildRequires: clang
BuildRequires: llvm
BuildRequires: llvm-devel
BuildRequires: postgresql-devel

Requires: postgresql%{pgversion}-server
%global __requires_exclude ^libdb2.*\\.so

%description
db2_fdw is a PostgreSQL extension that provides a Foreign Data Wrapper for easy and efficient
access to DB2 databases, including pushdown of WHERE conditions and required columns as well
as comprehensive EXPLAIN support.

%install

cd /host
make clean
make
%make_install

#mkdir -p %{buildroot}/usr/pgsql-%pgversion/lib/bitcode/db2_fdw/
#mkdir -p %{buildroot}/usr/pgsql-%pgversion/share/extension/
#mkdir -p %{buildroot}/usr/pgsql-%pgversion/doc/extension/
#
#%{__install} -m 0755 %{_builddir}/usr/pgsql-%pgversion/lib/db2_fdw.so                      %{buildroot}/usr/pgsql-%pgversion/lib/db2_fdw.so
#%{__install} -m 0644 %{_builddir}/usr/pgsql-%pgversion/lib/bitcode/db2_fdw/*               %{buildroot}/usr/pgsql-%pgversion/lib/bitcode/db2_fdw/
#%{__install} -m 0644 %{_builddir}/usr/pgsql-%pgversion/share/extension/db2_fdw*            %{buildroot}/usr/pgsql-%pgversion/share/extension/
#%{__install} -m 0644 %{_builddir}/usr/pgsql-%pgversion/share/extension/uninstall_db2_fdw*  %{buildroot}/usr/pgsql-%pgversion/share/extension/
#%{__install} -m 0644 %{_builddir}/usr/pgsql-%pgversion/doc/extension/db2_fdw.md            %{buildroot}/usr/pgsql-%pgversion/doc/extension/

%clean
rm -rf $RPM_BUILD_ROOT

%files
/usr/pgsql-%{pgversion}/*
