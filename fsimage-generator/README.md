# FSImage generator

Generates an FSImage file (via MiniDFSCluster) containing directories and files for testing and benching.

See [FSImageGenerator](src/main/java/de/m3y/hadoop/hdfs/hfsa/generator/FSImageGenerator.java) for details (directory depth, files per directory etc.)

### Building
```
mvn package
```

### Running
```bash
java -jar target/hfsa-fsimage-generator-1.2.1-SNAPSHOT.jar 
```
Example output:
```
2020-01-14 20:23:27,126 INFO  FSImageGenerator  - Max depth = 5, max width = 2, files-factor = 10
2020-01-14 20:23:27,127 INFO  FSImageGenerator  - Generates 806 dirs (depth up to 5) and 209560 files
2020-01-14 20:23:42,107 INFO  FSImageGenerator  - Progress: 100 directories and 26000 files...
2020-01-14 20:23:50,717 INFO  FSImageGenerator  - Progress: 200 directories and 52000 files...
...
2020-01-14 20:24:41,387 INFO  FSImageGenerator  - Progress: 800 directories / 208000 files...
2020-01-14 20:24:42,377 INFO  FSImageGenerator  - Created new FSImage containing meta data for 806 directories and 209560 files
2020-01-14 20:24:42,377 INFO  FSImageGenerator  - FSImage path : /Users/mm/projects/hfsa/fsimage-generator/fsimage.img
```
