%define name gafka
%define version 0.2.7
%define release 1
%define path usr
%define group Development/Tools
%define __os_install_post %{nil}

Summary:    Unified multi-datacenter multi-cluster kafka swiss-knife management console
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
Unified multi-datacenter multi-cluster kafka swiss-knife management console powered by golang.

%prep
mkdir -p $RPM_BUILD_DIR/%{name}-%{version}-%{release}
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}
git clone https://github.com/funkygao/gafka

%build
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/gafka
./build.sh -t gk
./build.sh -t zk
./build.sh -t ehaproxy
./build.sh -t kateway
./build.sh -t kguard

%install
export DONT_STRIP=1
rm -rf $RPM_BUILD_ROOT
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/gafka
mkdir -p $RPM_BUILD_ROOT/%{path}/bin
mkdir -p $RPM_BUILD_ROOT/etc/bash_completion.d
install cmd/gk/gk $RPM_BUILD_ROOT/%{path}/bin

%files
/%{path}/bin/gk
