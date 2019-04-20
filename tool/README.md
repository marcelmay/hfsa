# Hadoop FSImage Analyzer (HFSA) Tool

### Intro

The HFSA tool provides a summary overview of the HDFS data files and directories of users and groups
(answering 'who has how many/big/small files...').

### Installation

TODO

### Usage

#### Default (showing summary)
```
Analyze Hadoop FSImage file for user/group reports
Usage: hfsa-tool [-hV] [-v]... [-fun=<userNameFilter>] [-p=<dirs>[,
                 <dirs>...]]... FILE [COMMAND]
      FILE        FSImage file to process.
      -fun, --filter-by-user=<userNameFilter>
                  Filter user name by <regexp>.
  -h, --help      Show this help message and exit.
  -p, --path=<dirs>[,<dirs>...]
                  Directory path(s) to start traversing (default: [/]).
                    Default: [/]
  -v              Turns on verbose output. Use `-vv` for debug output.
                    Default: []
  -V, --version   Print version information and exit.
Commands:
  summary
Runs summary command by default.
```

#### Summary command
```
Usage: hfsa-tool summary [-s=<sort>]
  -s, --sort=<sort>   Sort by <fs> size, <fc> file count, <dc> directory count or
                        <bc> block count (default: fs).
                        Default: fs
```

### Requirements 

- JDK 1.8
- Hadoop 2.x FSImage

