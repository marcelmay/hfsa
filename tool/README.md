# Hadoop FSImage Analyzer (HFSA) Tool

### Intro

The HFSA tool provides a summary overview of the HDFS data files and directories of users and groups
(answering 'who has how many/big/small files...').

### Installation

1. Download [distribution](https://repo1.maven.org/maven2/de/m3y/hadoop/hdfs/hfsa/hfsa-tool/)
    1. Select latest version folder
    2. Download hfsa-tool-<VERSION>-bin.zip archive
2. Unpack archive
3. Run command  
   `hfsa-tool-<VERSION>-SNAPSHOT/bin/hfsa-tool`  
   You need a JDK 8+ installation and `java` in your `PATH`
   
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
  summary         Generates an HDFS usage summary (default command if no other
                    command specified)
  smallfiles, sf  Reports on small file usage
Runs summary command by default.
```

#### Example
```
> hfsa-tool-1.x/bin/hfsa-tool src/test/resources/fsi_small.img 

HDFS Summary : /
----------------

#Groups  | #Users      | #Directories | #Symlinks |  #Files     | Size [MB] | #Blocks   | File Size Buckets 
         |             |              |           |             |           |           | 0 B 1 MiB 2 MiB 4 MiB 8 MiB 16 MiB 32 MiB 64 MiB 128 MiB 256 MiB
----------------------------------------------------------------------------------------------------------------------------------------------------------
       3 |           3 |            8 |         0 |         11 |       331 |        12 |   0     2     1     2     1      0      2      1       1       1

By group:            3 | #Directories | #SymLinks | #File      | Size [MB] | #Blocks   | File Size Buckets
                       |              |           |            |           |           | 0 B 1 MiB 2 MiB 4 MiB 8 MiB 16 MiB 32 MiB 64 MiB 128 MiB 256 MiB
---------------------------------------------------------------------------------------------------------------------------------------------------------
                  root |            0 |         0 |          1 |         0 |         1 |   0     1     0     0     0      0      0      0       0       0
            supergroup |            8 |         0 |          8 |       151 |         8 |   0     1     1     2     1      0      1      1       1       0
                nobody |            0 |         0 |          2 |       180 |         3 |   0     0     0     0     0      0      1      0       0       1

By user:             3 | #Directories | #SymLinks | #File      | Size [MB] | #Blocks   | File Size Buckets
                       |              |           |            |           |           | 0 B 1 MiB 2 MiB 4 MiB 8 MiB 16 MiB 32 MiB 64 MiB 128 MiB 256 MiB
---------------------------------------------------------------------------------------------------------------------------------------------------------
                  root |            0 |         0 |          1 |         0 |         1 |   0     1     0     0     0      0      0      0       0       0
                   foo |            0 |         0 |          1 |       160 |         2 |   0     0     0     0     0      0      0      0       0       1
                    mm |            8 |         0 |          9 |       171 |         9 |   0     1     1     2     1      0      2      1       1       0

```
#### Summary sub command
```
Usage: hfsa-tool summary [-hV] [-s=<sort>]
Generates an HDFS usage summary (default command if no other command specified)
  -h, --help          Show this help message and exit.
  -s, --sort=<sort>   Sort by <fs> size, <fc> file count, <dc> directory count or
                        <bc> block count (default: fs).
                        Default: fs
  -V, --version       Print version information and exit.
```

#### Small files report sub command
```
Usage: hfsa-tool smallfiles [-hV] [--fsl=<fileSizeLimitBytes>]
                            [--uphl=<hotspotsLimit>]
Reports on small file usage
      --fsl, -fileSizeLimit=<fileSizeLimitBytes>
                  Small file size limit in bytes (IEC binary formatted, eg 2MiB).
                    Every file less equals the limit counts as a small file.
                    Default: 2097152
      --uphl, -userPathHotspotLimit=<hotspotsLimit>
                  Limit of directory hotspots containing most small files.
                    Default: 10
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
```

#### Example
Report on small files less than 3 megabytes, for all users matching regexp `m.*`:

```
> hfsa-tool-1.x/bin/hfsa-tool -fun="m.*" src/test/resources/fsi_small.img smallfiles --fsl 3MiB

Small files report (< 3 MiB)

Overall small files         : 4
User (filtered) small files : 3

#Small files  | Path (top 10) 
------------------------------
            4 | /
            3 | /test3
            2 | /test3/foo
            1 | /test3/foo/bar

Username | #Small files
-----------------------
mm       |            3

Username | Small files hotspots (top 10 count/path)
---------------------------------------------------
mm       |            3 | /
         |            2 | /test3
         |            1 | /test3/foo
         |            1 | /test3/foo/bar
---------------------------------------------------
```
### Requirements 

See [requirements](../README.md#requirements)
