# Hadoop FSImage Analyzer (HFSA) Tool

### Intro

The HFSA tool provides a summary overview of the HDFS data files and directories of users and groups
(answering 'who has how many/big/small files...').

### Installation

TODO

### Usage

```
Usage: hfsa-tool [-h] [-fun <userFilter>] [-s <sort>] FILE [DIRS...]
      FILE                    FSImage file to process.
      DIRS                    Directories to start traversing. Defaults to root
                                '/'
      -fun, --filter-by-user <userFilter>
                              Filter user name by <regexp>.
  -h, --help                  Displays this help message and quits.
  -s, --sort <sort>           Sort by <fs> size (default), <fc> file count,
                                <dc> directory count or <bc> block count.
```

### Requirements 

- JDK 1.8
- Hadoop 2.x FSImage

