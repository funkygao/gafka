# gafka RPM

### How to build gafka rpm

    yum install -y rpm-build
    rpmbuild -bb gafka.spec

### How to yum install without root priviledge on CentOS

    yumdownloader gafka
    # will download gafka-0.1.1-1.x86_64.rpm in the current directory
    rpm -ivh gafka-0.1.1-1.x86_64.rpm --prefix=/home/apple/mygafka
