# Hadoop FSImage Analyzer (HFSA)

The HFSA lib supports fast and partly multithreaded fsimage processing API file-, directory- and symlink aware visitor,
  derived from [Apache HDFS FSImageLoder](https://github.com/apache/hadoop/blob/master/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/FSImageLoader.java) )

### Example Usage

```
RandomAccessFile file = new RandomAccessFile("src/test/resources/fsi_small.img", "r");
FSImageLoader loader = FSImageLoader.load(file);
loader.visit(new FsVisitor() {
    @Override
    public void onFile(FsImageProto.INodeSection.INode inode, String path) {
        System.out.println("Visiting file " + inode.getName().toStringUtf8());
        // You can access size, permissions, ...
    }

    @Override
    public void onDirectory(FsImageProto.INodeSection.INode inode, String path) {
        System.out.println("Visiting directory " + inode.getName().toStringUtf8());
    }

    @Override
    public void onSymLink(FsImageProto.INodeSection.INode inode, String path) {
        System.out.println("Visiting sym link " + inode.getName().toStringUtf8());
    }
}, "/some/path");
```

See [HdfsFSIMageTool](../tool/src/main/java/de/m3y/hadoop/hdfs/hfsa/tool/HdfsFSImageTool.java) for a more advanced usage.
