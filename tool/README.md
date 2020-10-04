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
  -V, --version   Print version information and exit.
Commands:
  summary         Generates an HDFS usage summary (default command if no other
                    command specified)
  smallfiles, sf  Reports on small file usage
  inode, i        Shows INode details
  path, p         Lists INode paths
Runs summary command by default.
```

#### Example
```
> hfsa-tool src/test/resources/fsi_small.img 

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
> hfsa-tool -fun="m.*" src/test/resources/fsi_small.img smallfiles --fsl 3MiB

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
#### Show INode details 

Show details of selected INode, eg by directory path or file path or inode ID:
```
> hfsa-tool src/test/resources/fsi_small.img inode "/test3" "/test3/test_160MiB.img"
type: DIRECTORY
id: 16388
name: "test3"
directory {
  modificationTime: 1497734744891
  nsQuota: 18446744073709551615
  dsQuota: 18446744073709551615
  permission: 1099511759341
}

type: FILE
id: 16402
name: "test_160MiB.img"
file {
  replication: 1
  modificationTime: 1497734744886
  accessTime: 1497734743534
  preferredBlockSize: 134217728
  permission: 5497558401444
  blocks {
    blockId: 1073741834
    genStamp: 1010
    numBytes: 134217728
  }
  blocks {
    blockId: 1073741835
    genStamp: 1011
    numBytes: 33554432
  }
  storagePolicyID: 0
}
```

#### Lists INode paths
Lists all INode paths (files, directories, symlinks) similliar to a recursive 'ls'.

Example filtering user with regexp `m.*` and for paths `/test3` and `/test1` :
```
> hfsa-tool -fun="m.*" -p "/test3","/test1" src/test/resources/fsi_small.img p

Path report (paths=[/test3, /test1], user=~m.*) :
-------------------------------------------------

8 files, 4 directories and 0 symlinks

drwxr-xr-x mm supergroup /test1/test1
drwxr-xr-x mm supergroup /test3/foo
drwxr-xr-x mm supergroup /test3/foo/bar
-rw-r--r-- mm nobody     /test3/foo/bar/test_20MiB.img
-rw-r--r-- mm supergroup /test3/foo/bar/test_2MiB.img
-rw-r--r-- mm supergroup /test3/foo/bar/test_40MiB.img
-rw-r--r-- mm supergroup /test3/foo/bar/test_4MiB.img
-rw-r--r-- mm supergroup /test3/foo/bar/test_5MiB.img
-rw-r--r-- mm supergroup /test3/foo/bar/test_80MiB.img
-rw-r--r-- mm supergroup /test3/foo/test_20MiB.img
-rw-r--r-- mm supergroup /test3/test.img
drwxr-xr-x mm supergroup /test3/test3

```
### Requirements 

See [requirements](../README.md#requirements)
