<h2>How to build Netflix's Lipstick with CDH v5</h2>

Lipstick is a powerful tool to visualize what is happening with your pig execution, but it requires a few tweaks in order to build and execute against CDH5.

<h3>Prerequisites</h3>
* Working install of Centos 6
* Working install of CDH5 (basic HDFS and pig capabilities)
* Internet connectivity

<h3>What to do</h3>
<h4>Add some additional Centos packages</h4>
As root or via sudo 
<pre>yum -y install git mysql-server mysql
Set a password on your database</pre>

<h4>Add additional repositories</h4>
<pre>wget http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
rpm -Uvh epel-release-6*.rpm
wget http://www.graphviz.org/graphviz-rhel.repo
mv graphviz-rhel.repo /etc/yum.repos.d/</pre>

<h4>Install Graphiz</h4>
<pre>yum install 'graphviz*'</pre>

<h4>Create Lipstick Database</h4>
<pre>mysql -u root -p
create database lipstick;
grant all privileges on lipstick.* to lipstick@'%' identified by 'lipstick’;
flush privileges;</pre>

<h4>Clone the Lipstick project</h4>
<pre>git clone https://github.com/Netflix/Lipstick.git</pre>

<h4>Modify build.gradle to use CDH</h4>
see [build.gradle](https://github.com/matt-davies/uhug/blob/master/2014-06-Can_A_Pig_Wear_Lipstick/build.gradle)

<h4>Update BuildConfig.groovy to search additional repositories</h4>
see [BuildConfig.groovy](https://github.com/matt-davies/uhug/blob/master/2014-06-Can_A_Pig_Wear_Lipstick/BuildConfig.groovy)

file is located at lipstick-server/grails-app/conf/BuildConfig.groovy

<h4>Build Lipstick</h4>
<pre>./gradlew --stacktrace --debug --info</pre>










