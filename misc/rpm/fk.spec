%define name fk
%define version 0.2.6
%define release 3
%define path usr
%define group Development/Tools
%define __os_install_post %{nil}

Summary:    ffan.com kafka console for programmers
Name:       %{name}
Version:    %{version}
Release:    %{release}
Group:      %{group}
Packager:   Funky Gao <funky.gao@gmail.com>
License:    Apache
BuildRoot:  %{_tmppath}/%{name}-%{version}-%{release}
Prefix:     /usr
AutoReqProv: no
# we just assume you have go installed. You may or may not have an RPM to depend on.
# BuildRequires: go

%description 
ffan.com kafka console for programmers powered by golang.

%prep
mkdir -p $RPM_BUILD_DIR/%{name}-%{version}-%{release}
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}
git clone https://github.com/funkygao/gafka

%build
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/gafka
./build.sh -t fk

%install
export DONT_STRIP=1
rm -rf $RPM_BUILD_ROOT
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/gafka
mkdir -p $RPM_BUILD_ROOT/%{path}/bin
mkdir -p $RPM_BUILD_ROOT/etc/bash_completion.d
install cmd/fk/fk $RPM_BUILD_ROOT/%{path}/bin

%files
/%{path}/bin/fk
