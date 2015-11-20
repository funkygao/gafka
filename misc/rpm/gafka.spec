%define name gafka
%define version 0.1.0
%define release 1
%define path usr/local
%define group Database/Applications
%define __os_install_post %{nil}

Summary:    gafka
Name:       %{name}
Version:    %{version}
Release:    %{release}
Group:      %{group}
Packager:   Funky Gao <funky.gao@gmail.com>
License:    Apache
BuildRoot:  %{_tmppath}/%{name}-%{version}-%{release}
AutoReqProv: no
# we just assume you have go installed. You may or may not have an RPM to depend on.
# BuildRequires: go

%description 
gafka - Simplified multi-datacenter multi-kafka-clusters management console powered by golang.
https://github.com/funkygao/gafka

%prep
mkdir -p $RPM_BUILD_DIR/%{name}-%{version}-%{release}
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}
git clone git@github.com:funkygao/gafka.git

%build
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/gafka
./build.sh

%install
export DONT_STRIP=1
rm -rf $RPM_BUILD_ROOT
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/gafka
./build.sh -install

%files
/%{path}/bin/gk
