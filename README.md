Aegisthus
=========

STATUS
------

Aegisthus has been archived and will receive no further updates.

OVERVIEW
--------

A Bulk Data Pipeline out of Cassandra.  Aegisthus implements a reader for the 
SSTable format and provides a map/reduce program to create a compacted snapshot
of the data contained in a column family.

BUILDING
--------

Aegisthus is built via Gradle (http://www.gradle.org). To build from the command line:
    ./gradlew build

RUNNING
-------

Please [see the wiki](https://github.com/Netflix/aegisthus/wiki) or checkout the scripts
directory to use our sstable2json wrapper for individual sstables.

TESTING
-------

To run the included tests from the command line:
    ./gradlew build

ENHANCEMENTS
------------

* Reading
  * Commit log readers
    * Code to do this previously existed in Aegisthus but was removed in commit [35a05e3f](https://github.com/Netflix/aegisthus/commit/35a05e3fd02a016e61ea6ec833c5dbbf22feceac).
  * Split compressed input files
    * Currently compressed input files are only handled by a single mapper.  See the [discussion in issue #9](https://github.com/Netflix/aegisthus/issues/9).  The relevant section of code is in [getSSTableSplitsForFile in AegisthusInputFormat](https://github.com/Netflix/aegisthus/blob/1343de5b389c5a846d8509102078e3ca0680bedf/aegisthus-hadoop/src/main/java/com/netflix/aegisthus/input/AegisthusInputFormat.java#L74).
  * Add CQL support
    * This way the user doesn't have to add the key and column types as job parameters.  Perhaps we will do this by requiring the table schema like SSTableExporter does.
* Writing
  * Add an option to snappy compress output.
  * Add an output format for easier downstream processing.
    * See discussion on issue [#36](https://github.com/Netflix/aegisthus/issues/36).
  * Add a pivot format
    * Create an output format that contains a column per row. This can be used to support very large rows without having to have all of the columns in memory at one time.
* Packaging
  * Publish Aegisthus to Maven Central
  * Publish Shaded/Shadowed/FatJar version of Aegisthus as well

LICENSE
--------

Copyright 2013 Netflix, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
