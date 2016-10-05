# EMC Isilon Resource Plugin for iRODS
### Version 1.0

## Table of contents
- [Description](#description)
- [Core Features](#core-features)
- [Installing](#installing)
- [Configuring](#configuring)
- [Limitations and known problems](#limitations-and-known-problems)
- [External docs](#external-docs)
- [License](#license)

## Description
With this driver an EMC Isilon cluster can be registered into an iRODS grid as an
object based storage resource. I/O to the resource is handled via the HDFS interface.

This driver does not require a Java runtime environment to use the HDFS interface.
Instead the 'Hadoofus' library is required.

This driver can be used to integrate any HDFS based storage resource with iRODS.
However, be aware that it has been tested with Isilon only. We have no knowledge
about how it may or may not work with other HDFS storage devices.

In the future we may add features and optimizations which are Isilon-specific. Other
protocols supported by Isilon along with HDFS may also be utilized and could change
the suitability of this driver as a generic HDFS interface driver for iRODS.

## Core Features
* A storage resource driver for Isilon on iRODS 4.1.9 and later
* Object based access to an Isilon cluster: HDFS access which is JRE free
* Better load balancing and in some cases better performance compared to NFS access
(see first link under [External docs](#external-docs) below)
* Support for using a single Isilon storage pool across multiple iRODS resource
servers
* Some client-side performance optimizations like read ahead

## Installing
The current version of this plugin requires iRODS version 4.1.9 or higher.

There are three options to install the plugin:

1. install from a pre-built package
2. build the package from the source code and install the resulting package
3. build the plugin alone (without packaging) and put it in the appropriate place
manually

<b>NOTE: </b> if you use one of the first two options, upon installation completion,
you'll find product documentation (including this README.md file) in
`/usr/share/doc/libisilon`.

### Installing from a pre-built package
1. download the package (if available for your type and version of OS) from  
	```not available yet```
2. install it using respective package manager. For example,  
	on Ubuntu:  
	```sudo dpkg -i irods-resource-plugin-isilon*.deb```
  
	on CentOS:  
	```rpm -i irods-resource-plugin-isilon*.rpm```  
  
	You should install the plugin on every iRODS Resource server that you are
	going to access Isilon from
3. create and configure corresponding iRODS resource (see [Configuring](#configuring)
section below for details)

### Installing from manually built package
1. Install the iRODS Development Tools for your version of iRODS and OS from
[http://irods.org/download/](http://irods.org/download/)
2. Install the Hadoofus library from
[https://github.com/cemeyer/hadoofus](https://github.com/cemeyer/hadoofus) 
  
	This Hadoofus library is required to connect with the Isilon HDFS interface.
	We recommend using the <b> HEAD </b> version of the library

	<b> NOTE: </b> you need to install the iRODS Development Tools and Hadoofus
	**only** on the machine where you are going to build the plugin. You **do
	not need** to install the libraries on every machine where you will use the
	plugin. Techically, you do need the Hadoofus library on each machine which
	will run the plugin, but it is included in the package along with the
	resource plugin to save time and effort.
3. `git clone https://github.com/Sk-iRODS-Extensions/EMC-Isilon-Resource-Plugin-for-iRODS.git`
4. `cd irods_resource_plugin_isilon/packaging`
5. `./build`
6. When the build is complete, you can find the package in the `irods_resource_plugin_isilon/build`
directory
7. Install the package using the respective package manager. For example,  
	on Ubuntu:  
	```sudo dpkg -i irods-resource-plugin-isilon*.deb```
  
	on CentOS:  
	```rpm -i irods-resource-plugin-isilon*.rpm```

	<b>NOTE: </b> You **MUST** install the plugin on every iRODS  server that
	you are going to access the Isilon from.
8. Create and configure corresponding iRODS resource (see [Configuring](#configuring)
section below for details)

### Installing manually built plugin alone
1. Install the iRODS Development Tools for your version of iRODS and OS from
[http://irods.org/download/](http://irods.org/download/)
2. Install the Hadoofus library from
[https://github.com/cemeyer/hadoofus](https://github.com/cemeyer/hadoofus)
  
	This library is required to connect with the Isilon HDFS interface. We
	recommend using the <b> HEAD </b> version of the library
  
	<b> NOTE: </b> you need to install iRODS Development Tools **only** on the
	machine where you are going to build the plugin. You **do not need** to
	install it on every machine where you are going to use the plugin, but you
	**must** install Hadoofus manually on each iRODS server which will access the
	Isilon cluster.
3. `git clone https://github.com/Sk-iRODS-Extensions/EMC-Isilon-Resource-Plugin-for-iRODS.git`
4. `cd irods_resource_plugin_isilon`
5. `make` or `make debug` 
  
	Using the Debug target is extremely useful for debugging the plugin and logging
	its activity. When built with `debug` support, the plugin executes internal
	sanity checks during its work and also logs its activity to the standard iRODS
	log file. Corresponding lines of the file will be marked with the `ISILON RESC`
	prefix
6. When the compilation process is finished, you can find the `libisilon.so` file in
the source code upper level directory. Copy this file to `{iRODS_home}/plugins/resources/`
7. Create and configure corresponding iRODS resource (see [Configuring](#configuring)
section below for details)

## Configuring
The Isilon resource can be registered either as a first class resource or as part of
a hierarchical resource. In both cases some limitations may apply in the use of the
Isilon resource. See the [Limitations and known problems](#limitations-and-known-problems)
section below for more information.

Since archival is an important use case for Isilon storage system, we want to
explicitly state here that no limitations apply to using Isilon iRODS resource as an
`archive` resource in part of an iRODS `compound` resource definition.

To register an Isilon resource as a first class resource follow the instructions below.
Refer to the iRODS documentation for instructions on compound/hierarchical resource
administration.

1. Make sure that HDFS license is enabled on the Isilon storage array. It is required
since the plugin communicates with Isilon cluster through this interface
2. If you installed the Isilon resource plugin on the ICAT server using a package
manager, you may skip this step. Otherwise (i.e. if you did not install the plugin on
the ICAT server or did not use a package manager) you should add the following rule to
iRODS rule base (`/etc/irods/core.re` file):
```
acSetNumThreads { ON($KVPairs.rescType == "isilon") { msiSetNumThreads("default", "1","default"); } }
```
  
	**NOTE:**  If you're not experienced with iRODS rules, you should add this
	rule before any other `acSetNumThreads` rules
  
	**NOTE:** If you installed the plugin to ICAT server using package manager, the
	rule should have been added automatically
  
	**NOTE:** You may want to not use this rule at all. Please refer to
	[Limitations and known issues](#limitations-and-known-issues) section below to
	get more information about this rule   
3. Register the Isilon resource plugin with command:
```
iadmin mkresc <resource name> isilon localhost:/<path> "isi_host=<isilon management adress>;isi_port=8020;isi_user=root"
```
  
	Where:
	-  `<resource name>` is logical resource name used later in the system  
	- `<path>` is a path on the isilon file system relative to the `/ifs` directory.
	Your iRODS data will be stored under this path
	- `<isilon management adress>` is the Isilon SmartConnect management address
	(if SmartConnect is configured. Otherwise you can use any address assigned to
	the Isilon cluster)

## Limitations and known problems
Some iRODS functionality cannot be used  with Isilon resource plugin. That is
because of two reasons:

1. The Isilon plugin uses the efficient object based HDFS protocol to access the Isilon
storage system. This protocol doesn't support random write access (full POSIX semantics), but only
allows streaming writes.
2. Some iRODS functionality is limited to a `unix file system` resource only.
No other storage resource type can support it. In this respect, the Isilon resource plug-in is
similar to other "non-POSIX" resource drivers in the iRODS world.

### Isilon-specific limitation, constraints imposed by iRODS, and  areas for improvement:

**1)  Random writes**

Random writes can be explicit or implicit. In the first case they are intended by a user (e.g. they may come from some tricky rule). In second case they are a side effect of operations that could be potentially implemented without random writes. In first case there is no way to use Isilon plugin for random write workloads. In the second case, most random writes would come from an ordinary `iput` operation. Random writes can be avoided by forcing single-threaded mode operation for the iRODS commands `iget` and `iput`. This behavior is achieved by insertion of the following rule into the iRODS rules file,  `/etc/irods/core.re`, as part of the plugin installation.

```acSetNumThreads { ON($rescType == "isilon") { msiSetNumThreads("default","1","default"); } }```
	
This rule is also invoked on the `iget` command forcing it to work in single threaded mode.

Use of this rule comes at expense of some performance compared to accessing Isilon storage system as a `unix file system` resource (this applies to single-client performance only; we do not find any deficiencies in terms of aggregate multi-client performance; instead improvements are expected in this case). If the iRODS administrator determines it is necessary to run the `iget` command in multi-threaded mode (e.g. to recover single-client performance) that can be reactivied by removing or commenting out the line above in the iRODS rule file: `/etc/irods/core.re`.

If removal of this rule is done it will be necessary to explicitly restrict the behavior of the `iput` command, at each invocation by adding the parameter `-N 0` to the command line,  e.g.:

`iput -N 0 abc.txt`

A better rule will be provided in the  future restricting the `iput` command only while making no effect to `iget`.

**2) Isilon cluster authorizarion and access management**

The EMC Isilon Plugin for iRODS currently supports only basic authorization with no user level authentication.  For proper operation use the Isilon `root` account to register the Isilon as iRODS storage resource. This is area for improvement.

**3) Resource registration diagnostic**

The Isilon plugin (as any other plugin) cannot report errors caused by wrong parameters or physical resource status during the registration phase due to a defect in iRODS [https://github.com/irods/irods/issues/2336](https://github.com/irods/irods/issues/2336).  This problem is detected and reported only during active command execution.

**4) Quotas**

The plugin does not implement interface for reporting the space available to iRODS user. This is area for improvement.

**5) Replication**

When an Isilon storage resource is used as a target for replication the number of data transfer threads on the `irepl` command should be set manually to zero by adding the command line option: `-N 0`.

The iRODS utility `irepl` uses multi-stream data-transfers by default. When an Isilon storage resource is the target of a replication, multi-stream `irepl` will fail because it's built upon random writes which are not supported currently by Isilon storage resource. The number of data streams used by `irepl` for transferring data is not affected by the `acSetNumThreads` rule discussed above. That's why one needs to use `-N 0`	option explicitly each time Isilon resource is the target of `irepl`.

The `-N 0` option is not required if an Isilon storage resource is specified as a source for a replication and does not simultaneously appear as a target for replication.

**6) No support for cache resource**

The EMC Isilon plugin for iRODS does not allow using the Isilon as `cache` type of iRODS resource within a `compound` resource. This is a limitation imposed by iRODS. No resource type except `unix file system` can be used to represent `cache`. The issue is tracked at [https://github.com/irods/irods/issues/2249](https://github.com/irods/irods/issues/2249)

**7) Limited support for aliases**

It is possible to create multiple iRODS storage resources in the same iRODS zone which point to the same physical Isilon storage under multiple names in iRODS. Consider the following examples:

1. The same Isilon physical storage registered from different iRODS resource servers with different iRODS storage resource names:
	```iadmin mkresc isiResc1 isilon irods_host1:/vault_path "isi_host=vvv.xxx.yyy.zzz;isi_port=8020;isi_user=root"```

	```iadmin mkresc isiResc2 isilon irods_host2:/vault_path "isi_host=vvv.xxx.yyy.zzz;isi_port=8020;isi_user=root"```

2. The same Isilon physical storage registered from the same host using different iRODS storage resource names:
	```iadmin mkresc isiResc1 isilon irods_host:/vault_path "isi_host=vvv.xxx.yyy.zzz;isi_port=8020;isi_user=root"```

	```iadmin mkresc isiResc2 isilon irods_host:/vault_path "isi_host=vvv.xxx.yyy.zzz;isi_port=8020;isi_user=root"```

In both examples above resources `isiResc1` and `isiResc2` refer to the same space on the Isilon storage array while iRODS consider them as independent resources.

	
We recommend against using such constructs as it becomes confusing for the iRODS administrator, as well as user, to know the physical location of storage via the storage resource name.

**8) Bundle commands support**

The iRODS commands `ibun`, `iphybun` are not supported due to a limitation in iRODS tracked at: [https://github.com/irods/irods/issues/2183](https://github.com/irods/irods/issues/2183). The only resource type that supports these commands is `unix file system`.

## External docs
1. Resource Driver for EMC Isilon: implementation details and performance
evaluation in "Proceedings of iRODS User Group Meeting 2015" (page 69) at
[http://irods.org/wp-content/uploads/2015/09/UMG2015_P.pdf](http://irods.org/
wp-content/uploads/2015/09/UMG2015_P.pdf) 
2. EMC: Isilon and ECS Resource. Video from iRODS User Group Meeting 2015
located at [https://www.youtube.com/watch?v=5zo-t27u9Nk](https://www.youtube.
com/watch?v=5zo-t27u9Nk )
3. EMC - iRODS resource drivers. Slides used for a talk at iRODS User Group Meeting 2015.The slides are located at [https://irods.org/wp-content/uploads/
2015/06/Combes-iRODSResourceDrivers.pdf]( https://irods.org/wp-content/uploads/
2015/06/Combes-iRODSResourceDrivers.pdf)

## License
Copyright © 2016 EMC Corporation

This software is provided under the Apache 2.0 Software license provided in the [LICENSE.md](LICENSE.md) file. Licensing information for third-party products used by EMC Isilon resource plugin for iRODS can be found in [THIRD_PARTY_SOFTWARE_README.md](THIRD_PARTY_SOFTWARE_README.md)
