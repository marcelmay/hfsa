# Hadoop FSImage Analyzer (HFSA)

### Intro

Hadoop FSImage Analyzer (HFSA) complements the [Apache Hadoop 'hadoop-hdfs' tool](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HDFSCommands.html)
by providing [HDFS fsimage](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html#The_Persistence_of_File_System_Metadata)
* [tooling](tool) support for summary overview of the HDFS data files and directories of users and groups
  (answering 'who has how many/big/small files...')
* a [library](lib) for fast and partly multithreaded fsimage processing API file-, directory- and symlink aware visitor,
  derived from [Apache HDFS FSImageLoder](https://github.com/apache/hadoop/blob/master/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/FSImageLoader.java) )

TODO: Example 

### Requirements

- JDK 1.8
- Hadoop 2.x

### TODO

It's an initial, drafty version.
- Documentation
- Deployment to Maven repo
- error handling and cli options for tool
- report and config options for topk/sorting/selection/...
- report /user/*/.Trash etc per directory

### License

HFSA is released under the [Apache 2.0 license](LICENSE.txt).

```
Copyright 2017 Marcel May

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
```

Contains work derived from Apache Hadoop.
