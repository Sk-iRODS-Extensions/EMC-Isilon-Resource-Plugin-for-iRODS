# EMC Isilon Resource Plugin for iRODS
### Version 1.0

## Table of contents
- [Description](description)
- [Core Features](core-features)
- [Installing](installing)
- [Configuring](configuring)
- [Limitations and known problems](limitations-and-known-problems)
- [External docs](external-docs)
- [License](license)

## Description
With this driver an EMC Isilon cluster can be registered into an iRODS grid
as an object based storage resource. I/O to the resource is handled via HDFS
interface.

The driver does not require a Java runtime environment to use the HDFS
interface. Instead the 'Hadoofus' library is required.

The driver supposedly can be used to integrate any HDFS based storage resource
with iRODS. Be aware that we tested it with Isilon only. In future we may add
features and optimizations that are Isilon-specific. Other protocols supported
by Isilon along with HDFS may also be utilized

## Core Features
* Storage resource driver for Isilon on iRODS 4.1.9 and later
* Object based access to Isilon cluster: HDFS access which is JRE free
* Better load balancing and in some cases better performance compared to NFS
access (see first link under [External docs](#external-docs) below)
* Support for using a single Isilon storage pool across multiple iRODS resource
servers
* Some client-side performance optimizations like read ahead

## Installing
The current version of this plugin requires iRODS version 4.1.9 or higher.

You have three options to install the plugin:
1. install from pre-built package
2. build package yourself from the source code and install from it
3. build the plugin alone (without packaging) and put it in appropriate
place manually

NOTE: if you use one of the first two options, upon installation completion,
you'll find product documentation (including this README.md file) in
`/usr/share/doc/libisilon`.

### Installing from pre-built package
1. download the package (if available for your type and version of OS) from
	```not available yet```
2. install it using respective package manager. For example,
	on Ubuntu:
	```sudo dpkg -i irods-resource-plugin-isilon*.deb```

	on CentOS:
    ```rpm -i irods-resource-plugin-isilon*.rpm```

	You should install the plugin on every iRODS Resource server that you
	are going to access Isilon from
3. create and configure corresponding iRODS resource (see [Configuring](#configuring)
section below for details)

### Installing from manually built package
1. install iRODS Development Tools for your version of iRODS and OS from
[http://irods.org/download/](http://irods.org/download/)
2. install Hadoofus library from [https://github.com/cemeyer/hadoofus](https:
//github.com/cemeyer/hadoofus)
This library is required to connect with Isilon HDFS interface.
We recommend to use HEAD version of the library

NOTE: you need to install iRODS Development Tools and Hadoofus **only** on a
machine where you're going to build the plugin. You **don't need** to install
them on every machine where you're going to use the plugin. Techically,
you do need Hadoofus on each of those machines, but it will be included in
the package along with the resource plugin.

3. `git clone *path to the plugin Git repository*`
4. `cd irods_resource_plugin_isilon/packaging`
5. `./build`
6. when build is complete, you can find the package in
`irods_resource_plugin_isilon/build` directory
7. install it using respective package manager. For example,
	on Ubuntu:
	```sudo dpkg -i irods-resource-plugin-isilon*.deb```

	on CentOS:
    ```rpm -i irods-resource-plugin-isilon*.rpm```

	You should install the plugin on every iRODS Resource server that you
	are going to access Isilon from
8. create and configure corresponding iRODS resource (see [Configuring](#configuring)
section below for details)

### Installing manually built plugin alone
1. install iRODS Development Tools for your version of iRODS and OS from
[http://irods.org/download/](http://irods.org/download/)
2. install Hadoofus library from [https://github.com/cemeyer/hadoofus](https:
//github.com/cemeyer/hadoofus)
This library is required to connect with Isilon HDFS interface.
We recommend to use HEAD version of the library

NOTE: you need to install iRODS Development Tools **only** on a machine where
you're going to build the plugin. You **don't need** to install it on every
machine where you're going to use the plugin. But you **do** need to install
Hadoofus manually on every such machine.

3. `git clone *path to the plugin Git repository*`
4. `cd irods_resource_plugin_isilon`
5. `make` or `make debug`. Debug target is extremely useful for debugging the
plugin and logging its activity. When built with `debug` support, the plugin
executes internal sanity checks during its work and also logs its activity to
standard iRODS log file. Corresponding lines of the file will be marked with
'ISILON RESC' prefix
6. when compilation process is finished, you can find `libisilon.so` file in
the source code upper level directory. Copy this file to
`{iRODS_home}/plugins/resources/`
7. create and configure corresponding iRODS resource (see [Configuring](#configuring)
section below for details)

## Configuring
Isilon resource can be registered either as a first class resource or as part
of a hierarchical resource. In both cases some limitations may apply to use of
Isilon resource. See [Limitations and known issues](#limitations-and-known-issues)
section below for more information.

Since archival is important use case for Isilon storage system, we want to
explicitly state here that no limitations apply to using Isilon iRODS resource as
`archive` part of `compound` resource.

Below is what you need to do to register first class Isilon resource. Refer to
the iRODS documentation for instructions on compound/hierarchical resource
administration.

1. Make sure that HDFS license is enabled on the Isilon storage array. It's
required since the plugin communicates with Isilon cluster through this interface
2. If you installed the plugin to ICAT server using package manager, you may
skip this step. Otherwise (i.e. if you didn't install the plugin to ICAT
machine or didn't use package manager) you should add the following rule to iRODS rule
base (`/etc/irods/core.re` file):

```
acSetNumThreads { ON($KVPairs.rescType == "isilon") { msiSetNumThreads("default", "1","default"); } }
```

	NOTE: if you're not experienced with iRODS rules, you should add this rule before
	any other `acSetNumThreads` rules

	NOTE: if you installed the plugin to ICAT server using package manager, the
	rule should have been added automatically

	NOTE: you may want to not use this rule at all. Please refer to [Limitations
	and known issues](#limitations-and-known-issues) section below to get more
	information about this rule   
3. Register the plugin with command:
```
iadmin mkresc <resource name> isilon localhost:/<path> "isi_host=<isilon
management adress>;isi_port=8020;isi_user=root"
```

	Where:
	* `<resource name>` is logical resource name used later in the system
	* `<path>` is a path on the isilon file system relative to the /ifs directory.
	Your iRODS data will be stored under this path
	* `<isilon management adress>` is the Isilon SmartConnect management address
	(if SmartConnect is configured. Otherwise you can use any address assigned to
	Isilon cluster)

## Limitations and known problems
Some iRODS functionality cannot be used along with Isilon resource. That's
because of two reasons:
1. Isilon plugin uses efficient object based HDFS protocol to access Isilon
storage system. This protocol doesn't support random write access (but only
allows streaming writes);
2. Some iRODS functionality is limited to `unix file system` resource only.
No other resource type can support it. In this respect, Isilon resource is
not any worse than any other resource which type is different from `unix file
system`.

Below you can find details about Isilon-specific limitation, constraints
imposed by iRODS, and some areas for improvement:
1. **Random writes**
	Random writes can be explicit or implicit. In first case they are intended
	by a user (e.g. come from some tricky rule). In second case they are side
	effect of operations that could be potentially implemented without random writes.
	In first case there is no way to use Isilon plugin for random write workloads.
	As to the second case, then most random writes come from ordinary `iput`
	operation. They can be avoided by forcing single-threaded mode operation for
	the iRODS commands `iget` and `iput`. This behavior is achieved by insertion
	of the following rule into the iRODS rules file,  `/etc/irods/core.re`, as
	part of the plugin installation.

	```acSetNumThreads { ON($rescType == "isilon") { msiSetNumThreads("default","1","default"); } }```
	This rule is also invoked on the `iget` command forcing it to work in single
	threaded mode.
	Use of this rule comes at expense of some performance compared to accessing
	Isilon storage system as a `unix file system` resource (this applies to
	single-client performance only; we don't expect any deficiencies in terms of
	aggregate multi-client performance; instead improvements are expected in this
	case).
	If the iRODS administrator determines it is necessary to run the `iget` command
	in multi-threaded mode (e.g. to recover single-client performance) that can be
	reactivied by removing or commenting out the line above in the iRODS rule file:
	```/etc/irods/core.re```.

	If removal of this rule is done it will be necessary to explicitly restrict
	the behavior of the `iput` command, at each invocation by adding
	the parameter `-N 0` to the command line,  e.g.:

	`iput -N 0 abc.txt`

	A better rule will be provided in the nearest future. It will restrict
	`iput` command only while making no effect to `iget`.
2. **Isilon cluster authorizarion and access management**
	The EMC Isilon Plugin for iRODS currently supports only basic authorization
	with no user level authentication.  For proper operation use the Isilon `root`
	account to register the Isilon as iRODS storage resource.
	This is area for improvement.
3. **Resource registration diagnostic**
	The Isilon plugin (as any other plugin) cannot report errors caused by wrong
	parameters or physical resource status during the registration phase due to a
	defect in iRODS [https://github.com/irods/irods/issues/2336](https://github.com
	/irods/irods/issues/2336).
	The problem is detected and reported only during active command execution
4. **Quota**
	The plugin does not implement interface for reporting the space available
	to iRODS user.
	This is area for improvement.
5. **Replication**
	When an Isilon storage resource is used as a target for replication the number
	of data transfer threads on the `irepl` command should be set manually to zero by
	adding the command line option: `-N 0`.

	The iRODS utility `irepl` uses multi-stream data-transfers by default. When
	an Isilon storage resource is the target of a replication, multi-stream `irepl`
	will fail because it's built upon random writes which are not supported
	currently by Isilon storage resource. The number of data streams used by
	`irepl` for transferring data is not affected by the `acSetNumThreads` rule
	discussed above. That's why one needs to use `-N 0`	option explicitly each time
	Isilon resource is the target of `irepl`.

	The `-N 0` option is not required if an Isilon storage resource is specified
	as a source for a replication and does not simultaneously appear as a target
	for replication.
6. **No support for cache resource**
	EMC Isilon plugin for iRODS does not allow using the Isilon as `cache` type
	of iRODS resource within a `compound` resource. This is a limitation imposed
	by iRODS. No resource type except `unix file system` can be used to represent
	`cache`. The issue is tracked at [https://github.com/irods/irods/issues/2249](https://github.com/irods/irods/issues/2249)
7. **Limited support for aliases**
	It is possible to create multiple iRODS storage resources in the same iRODS zone
	which point to the same physical Isilon storage under multiple names in iRODS.
	Consider the following examples:
	1. The same Isilon physical storage registered from different iRODS resource
	servers with different iRODS storage resource names:
	```iadmin mkresc isiResc1 isilon irods_host1:/vault_path "isi_host=vvv.xxx.yyy.zzz;isi_port=8020;isi_user=root"```

	```iadmin mkresc isiResc2 isilon irods_host2:/vault_path "isi_host=vvv.xxx.yyy.zzz;isi_port=8020;isi_user=root"```

	2. The same Isilon physical storage registered from the same host using
	different iRODS storage resource names:
	```iadmin mkresc isiResc1 isilon irods_host:/vault_path "isi_host=vvv.xxx.yyy.zzz;isi_port=8020;isi_user=root"```

	```iadmin mkresc isiResc2 isilon irods_host:/vault_path "isi_host=vvv.xxx.yyy.zzz;isi_port=8020;isi_user=root"```

	In both examples above resources `isiResc1` and `isiResc2` refer to the same
	space on the Isilon storage array while iRODS consider them as independent
	resources.

	We recommend against using such constructs as it becomes confusing for the
	iRODS administrator, as well as user, to know the physical location of storage
	via the storage resource name.
8. **Bundle commands support**
	The iRODS commands `ibun`, `iphybun` are not supported due to a limitation
	in iRODS tracked at: [https://github.com/irods/irods/issues/2183](https://github.com/irods/irods/issues/2183)
	The only resource type that supports these commands is `unix file system`

## External docs
1. Resource Driver for EMC Isilon: implementation details and performance
evaluation in "Proceedings of iRODS User Group Meeting 2015" (page 69) at
[http://irods.org/wp-content/uploads/2015/09/UMG2015_P.pdf](http://irods.org/
wp-content/uploads/2015/09/UMG2015_P.pdf) 
2. EMC: Isilon and ECS Resource. Video from iRODS User Group Meeting 2015
located at [https://www.youtube.com/watch?v=5zo-t27u9Nk](https://www.youtube.
com/watch?v=5zo-t27u9Nk )
3. EMC - iRODS resource drivers. Slides used for a talk at iRODS User Group
4. Meeting 2015.The slides are located at [https://irods.org/wp-content/uploads/
2015/06/Combes-iRODSResourceDrivers.pdf]( https://irods.org/wp-content/uploads/
2015/06/Combes-iRODSResourceDrivers.pdf)

## License
Copyright © 2016 EMC Corporation

This software is provided under the Apache 2.0 Software license provided in
the [LICENSE.md](LICENSE.md) file. Licensing information for third-party products
used by EMC Isilon resource plugin for iRODS can be found in
[THIRD_PARTY_SOFTWARE_README.md](THIRD_PARTY_SOFTWARE_README.md)
