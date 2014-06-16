#How to build Netflix's Lipstick with CDH v5

Lipstick is a powerful tool to visualize what is happening with your pig execution, but it requires a few tweaks in order to build and execute against CDH5.

##Prerequisites
* Working install of Centos 6
* Working install of CDH5 (basic HDFS and pig capabilities)
* Internet connectivity

##What to do
**Add some additional Centos packages**

As root or via sudo `yum -y install git mysql-server mysql`

Set a password on your database

**Add additional repositories**

`wget http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm`

`rpm -Uvh epel-release-6*.rpm`

`wget http://www.graphviz.org/graphviz-rhel.repo`

`mv graphviz-rhel.repo /etc/yum.repos.d/`

**Install Graphiz**

`yum install 'graphviz*'`

**Create Lipstick Database**

`mysql -u root -p`

`create database lipstick;`

`grant all privileges on lipstick.* to lipstick@'%' identified by 'lipstick’;`

`flush privileges;`









