aegisthus
=========

A Bulk Data Pipeline out of Cassandra.  Aegisthus implements a reader for the 
SSTable format and provides a map/reduce program to create a compacted snapshot
of the data contained in a column family.

BUILDING
=========

aegisthus is built via Gradle (http://www.gradle.org). To build from the command line:
    ./gradlew build

RUNNING
=========

Please [see the wiki](https://github.com/Netflix/aegisthus/wiki) or checkout the scripts
directory to use our sstable2json wrapper for individual sstables.

LICENSE
=========

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
