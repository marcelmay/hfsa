package de.m3y.hadoop.hdfs.hfsa.core;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * fsi_small.img test data content:
 * <p>
 * perms / blocks / user / group /     size / modified      / full path
 * <pre>
 * drwxr-xr-x   - mm   supergroup          0 2017-07-22 09:58 /datalake
 * drwxr-xr-x   - mm   supergroup          0 2017-07-22 09:57 /datalake/asset1
 * drwxr-xr-x   - mm   supergroup          0 2017-07-22 10:01 /datalake/asset2
 * -rw-r--r--   1 mm   supergroup       1024 2017-07-22 10:00 /datalake/asset2/test_1KiB.img
 * -rw-r--r--   1 mm   supergroup    2097152 2017-07-22 10:01 /datalake/asset2/test_2MiB.img
 * drwxr-xr-x   - mm   supergroup          0 2017-07-22 10:01 /datalake/asset3
 * drwxr-xr-x   - mm   supergroup          0 2017-07-22 10:01 /datalake/asset3/subasset1
 * -rw-r--r--   1 mm   supergroup    2097152 2017-07-22 10:01 /datalake/asset3/subasset1/test_2MiB.img
 * drwxr-xr-x   - mm   supergroup          0 2017-07-22 10:01 /datalake/asset3/subasset2
 * -rw-r--r--   1 mm   supergroup    2097152 2017-07-22 10:01 /datalake/asset3/subasset2/test_2MiB.img
 * -rw-r--r--   1 mm   supergroup    2097152 2017-07-22 10:01 /datalake/asset3/test_2MiB.img
 * drwxr-xr-x   - mm   supergroup          0 2017-06-17 23:03 /test1
 * drwxr-xr-x   - mm   supergroup          0 2017-06-17 23:03 /test2
 * drwxr-xr-x   - mm   supergroup          0 2017-06-17 23:25 /test3
 * drwxr-xr-x   - mm   supergroup          0 2017-06-17 23:11 /test3/foo
 * drwxr-xr-x   - mm   supergroup          0 2017-06-17 23:25 /test3/foo/bar
 * -rw-r--r--   1 mm   nobody       20971520 2017-06-17 23:13 /test3/foo/bar/test_20MiB.img
 * -rw-r--r--   1 mm   supergroup    2097152 2017-06-17 23:10 /test3/foo/bar/test_2MiB.img
 * -rw-r--r--   1 mm   supergroup   41943040 2017-06-17 23:25 /test3/foo/bar/test_40MiB.img
 * -rw-r--r--   1 mm   supergroup    4145152 2017-06-17 23:10 /test3/foo/bar/test_4MiB.img
 * -rw-r--r--   1 mm   supergroup    5181440 2017-06-17 23:10 /test3/foo/bar/test_5MiB.img
 * -rw-r--r--   1 mm   supergroup   83886080 2017-06-17 23:25 /test3/foo/bar/test_80MiB.img
 * -rw-r--r--   1 root root             1024 2017-06-17 23:09 /test3/foo/test_1KiB.img
 * -rw-r--r--   1 mm   supergroup   20971520 2017-06-17 23:11 /test3/foo/test_20MiB.img
 * -rw-r--r--   1 mm   supergroup    1048576 2017-06-17 23:07 /test3/test.img
 * -rw-r--r--   1 foo  nobody      167772160 2017-06-17 23:25 /test3/test_160MiB.img
 * -rw-r--r--   1 mm   supergroup       2048 2017-07-08 08:00 /test_2KiB.img
 * drwxr-xr-x   - mm   supergroup          0 2017-06-17 23:04 /user
 * drwxr-xr-x   - mm   supergroup          0 2017-06-17 23:04 /user/mm
 *
 * </pre>
 */
public class FSImageLoaderTest {
    private static final Logger LOG = LoggerFactory.getLogger(FSImageLoaderTest.class);
    private FSImageLoader loader;

    private Set<String> groupNames = new HashSet<>();
    private Set<String> userNames = new HashSet<>();
    private int sumFiles;
    private int sumDirs;
    private int sumSymLinks;
    private long sumSize;

    @Before
    public void setUp() throws IOException {
        try (RandomAccessFile file = new RandomAccessFile("src/test/resources/fsi_small.img", "r")) {
            loader = FSImageLoader.load(file);
        }
    }

    @Test
    public void testLoadAndVisitParallel() throws IOException {
        loadAndVisit((FSImageLoader loader, FsVisitor visitor) -> {
            try {
                loader.visitParallel(visitor);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    @Test
    public void testLoadAndVisit() throws IOException {
        loadAndVisit((FSImageLoader loader, FsVisitor visitor) -> {
            try {
                loader.visit(visitor);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private void loadAndVisit(BiConsumer<FSImageLoader, FsVisitor> handleVisit) throws IOException {
        Set<String> paths = new HashSet<>();
        Set<String> files = new HashSet<>();

        final FsVisitor visitor = new FsVisitor() {
            @Override
            public void onFile(FsImageProto.INodeSection.INode inode, String path) {
                final String fileName = (FSImageLoader.ROOT_PATH.equals(path) ? path : path + '/') +
                        inode.getName().toStringUtf8();
                LOG.debug(fileName);
                files.add(fileName);
                paths.add(path);
                FsImageProto.INodeSection.INodeFile f = inode.getFile();
                PermissionStatus p = loader.getPermissionStatus(f.getPermission());
                groupNames.add(p.getGroupName());
                userNames.add(p.getUserName());
                sumFiles++;
                sumSize += FSImageLoader.getFileSize(f);
            }

            @Override
            public void onDirectory(FsImageProto.INodeSection.INode inode, String path) {
                final String dirName = (FSImageLoader.ROOT_PATH.equals(path) ? path : path + '/') +
                        inode.getName().toStringUtf8();
                paths.add(dirName);
                LOG.debug(dirName);
                FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
                PermissionStatus p = loader.getPermissionStatus(d.getPermission());
                groupNames.add(p.getGroupName());
                userNames.add(p.getUserName());
                sumDirs++;
            }

            @Override
            public void onSymLink(FsImageProto.INodeSection.INode inode, String path) {
                paths.add(path);
                sumSymLinks++;
            }
        };
        handleVisit.accept(loader, visitor);

        assertThat(userNames.size()).isEqualTo(3);
        assertThat(groupNames.size()).isEqualTo(3);
        assertThat(sumDirs).isEqualTo(14);
        assertThat(sumFiles).isEqualTo(16);
        assertThat(sumSymLinks).isEqualTo(0);
        assertThat(sumSize).isEqualTo(356409344L);

        String[] expectedPaths = new String[]{
                "/", "/test1", "/test2", "/test3", "/test3/foo", "/test3/foo/bar", "/user", "/user/mm",
                "/datalake", "/datalake/asset1", "/datalake/asset2",
                "/datalake/asset3", "/datalake/asset3/subasset1", "/datalake/asset3/subasset2"};
        assertThat(paths).containsExactlyInAnyOrder(expectedPaths);

        String[] expectedFiles = new String[]{
                "/test_2KiB.img",
                "/test3/test.img",
                "/test3/test_160MiB.img",
                "/test3/foo/test_1KiB.img",
                "/test3/foo/test_20MiB.img",
                "/test3/foo/bar/test_20MiB.img",
                "/test3/foo/bar/test_2MiB.img",
                "/test3/foo/bar/test_40MiB.img",
                "/test3/foo/bar/test_4MiB.img",
                "/test3/foo/bar/test_5MiB.img",
                "/test3/foo/bar/test_80MiB.img",
                "/datalake/asset2/test_1KiB.img",
                "/datalake/asset2/test_2MiB.img",
                "/datalake/asset3/subasset1/test_2MiB.img",
                "/datalake/asset3/subasset2/test_2MiB.img",
                "/datalake/asset3/test_2MiB.img"
        };
        assertThat(files).containsExactlyInAnyOrder(expectedFiles);

        assertThat(loader.getINodeFromPath("/test3/foo/bar/test_40MiB.img").getFile().getReplication()).isEqualTo(1);
        assertThat(loader.getINodeFromPath("/test3/foo/bar/test_80MiB.img").getFile().getReplication()).isEqualTo(3);
        assertThat(loader.getINodeFromPath("/test3/foo/bar/test_4MiB.img").getFile().getReplication()).isEqualTo(5);

        assertThat(loader.getNumChildren(loader.getINodeFromPath("/datalake"))).isEqualTo(3);
        assertThat(loader.getNumChildren(loader.getINodeFromPath("/test3"))).isEqualTo(3);
        assertThat(loader.getNumChildren(loader.getINodeFromPath("/test3/foo"))).isEqualTo(3);
        assertThat(loader.getNumChildren(loader.getINodeFromPath("/test3/foo/bar/"))).isEqualTo(6);
    }

    @Test
    public void testLoadAndVisitWithPath() throws IOException {
        Set<String> paths = new HashSet<>();
        Set<String> files = new HashSet<>();

        loader.visit(new FsVisitor() {
            @Override
            public void onFile(FsImageProto.INodeSection.INode inode, String path) {
                files.add(("/".equals(path) ? path : path + '/') + inode.getName().toStringUtf8());
                paths.add(path);
                FsImageProto.INodeSection.INodeFile f = inode.getFile();

                PermissionStatus p = loader.getPermissionStatus(f.getPermission());
                groupNames.add(p.getGroupName());
                userNames.add(p.getUserName());
                sumFiles++;
                sumSize += FSImageLoader.getFileSize(f);
            }

            @Override
            public void onDirectory(FsImageProto.INodeSection.INode inode, String path) {
                paths.add(("/".equals(path) ? path : path + '/') + inode.getName().toStringUtf8());
                FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
                PermissionStatus p = loader.getPermissionStatus(d.getPermission());
                groupNames.add(p.getGroupName());
                userNames.add(p.getUserName());
                sumDirs++;
            }

            @Override
            public void onSymLink(FsImageProto.INodeSection.INode inode, String path) {
                paths.add(path);
                sumSymLinks++;
            }
        }, "/test3");

        assertThat(userNames.size()).isEqualTo(3);
        assertThat(groupNames.size()).isEqualTo(3);
        assertThat(sumDirs).isEqualTo(3);
        assertThat(sumFiles).isEqualTo(10);
        assertThat(sumSymLinks).isEqualTo(0);
        assertThat(sumSize).isEqualTo(348017664L);

        String[] expectedPaths = new String[]{
                "/test3", "/test3/foo", "/test3/foo/bar"};
        assertThat(paths).containsExactlyInAnyOrder(expectedPaths);

        String[] expectedFiles = new String[]{
                "/test3/test.img",
                "/test3/test_160MiB.img",
                "/test3/foo/test_1KiB.img",
                "/test3/foo/test_20MiB.img",
                "/test3/foo/bar/test_20MiB.img",
                "/test3/foo/bar/test_2MiB.img",
                "/test3/foo/bar/test_40MiB.img",
                "/test3/foo/bar/test_4MiB.img",
                "/test3/foo/bar/test_5MiB.img",
                "/test3/foo/bar/test_80MiB.img"
        };
        assertThat(files).containsExactlyInAnyOrder(expectedFiles);
    }

    @Test
    public void testGetInodeFromPath() throws IOException {
        final FsImageProto.INodeSection.INode rootNode = loader.getINodeFromPath("/");
        // Root node has empty name
        assertThat(rootNode.getName().toStringUtf8()).isEqualTo("");
        assertThat(rootNode.getType()).isEqualTo(FsImageProto.INodeSection.INode.Type.DIRECTORY);

        final FsImageProto.INodeSection.INode test3Node = loader.getINodeFromPath("/test3");
        assertThat(test3Node.getName().toStringUtf8()).isEqualTo("test3");
        assertThat(test3Node.getType()).isEqualTo(FsImageProto.INodeSection.INode.Type.DIRECTORY);

        final FsImageProto.INodeSection.INode test3FooBarNode = loader.getINodeFromPath("/test3/foo/bar");
        assertThat(test3FooBarNode.getName().toStringUtf8()).isEqualTo("bar");
        assertThat(test3FooBarNode.getType()).isEqualTo(FsImageProto.INodeSection.INode.Type.DIRECTORY);

        final FsImageProto.INodeSection.INode fileNode = loader.getINodeFromPath("/test3/test_160MiB.img");
        assertThat(fileNode.getName().toStringUtf8()).isEqualTo("test_160MiB.img");
        assertThat(fileNode.getType()).isEqualTo(FsImageProto.INodeSection.INode.Type.FILE);

        // Behave like java.io.File (POSIX), which allows redundant slashes
        final FsImageProto.INodeSection.INode rootRootNode = loader.getINodeFromPath("//");
        assertThat(rootRootNode.getName().toStringUtf8()).isEqualTo("");

        final FsImageProto.INodeSection.INode r3Node = loader.getINodeFromPath("///");
        assertThat(r3Node.getName().toStringUtf8()).isEqualTo("");

        final FsImageProto.INodeSection.INode r3FileNode = loader.getINodeFromPath("///test3//test_160MiB.img");
        assertThat(r3FileNode.getName().toStringUtf8()).isEqualTo("test_160MiB.img");
    }

    @Test
    public void testGetChildPaths() throws IOException {
        List<String> childPaths = loader.getChildPaths("/");
        String[] expectedChildPaths = new String[]{"/user", "/test1", "/test2", "/test3", "/datalake"};
        assertThat(childPaths).containsExactlyInAnyOrder(expectedChildPaths);
    }

    @Test
    public void testGetFileINodesInDirectory() throws IOException {
        // Directory with no files but another directory
        List<FsImageProto.INodeSection.INode> files = loader.getFileINodesInDirectory("/user");
        assertThat(files.size()).isEqualTo(0);

        // Directory with two files
        files = loader.getFileINodesInDirectory("/test3");
        assertThat(files.size()).isEqualTo(2);
        final List<String> fileNames = files.stream().map((n) -> n.getName().toStringUtf8()).collect(Collectors.toList());
        assertThat(fileNames).contains("test.img");
        assertThat(fileNames).contains("test_160MiB.img");

        // Root has a single file
        files = loader.getFileINodesInDirectory("/");
        assertThat(files.size()).isEqualTo(1);

        // Invalid directory
        assertThatExceptionOfType(FileNotFoundException.class)
                .isThrownBy(() -> loader.getFileINodesInDirectory("/does-not-exist"));

        // Invalid directory : path is file
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> loader.getFileINodesInDirectory("/test3/test.img"));
    }

    @Test
    public void testHasINode() throws IOException {
        assertThat(loader.hasINode("/user")).isTrue();
        assertThat(loader.hasINode("/test3/test.img")).isTrue();
        assertThat(loader.hasINode("/does-not-exist")).isFalse();
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> loader.hasINode("invalid-path"));

        assertThat(loader.hasINode("/")).isTrue();
        assertThat(loader.hasINode("//")).isTrue();
    }

    @Test
    public void testHasChildren() throws IOException {
        assertThat(loader.hasChildren("/")).isTrue();
        assertThat(loader.hasChildren(loader.getINodeFromPath("/").getId())).isTrue();
        assertThat(loader.hasChildren("/user")).isTrue();
        assertThat(loader.hasChildren(loader.getINodeFromPath("/user").getId())).isTrue();
        assertThat(loader.hasChildren("/test3/foo/bar/")).isTrue();
        assertThat(loader.hasChildren(loader.getINodeFromPath("/test3/foo/bar/").getId())).isTrue();
        assertThat(loader.hasChildren("/test3/foo/bar")).isTrue();
        assertThat(loader.hasChildren(loader.getINodeFromPath("/test3/foo/bar").getId())).isTrue();
        assertThat(loader.hasChildren("/test1")).isFalse();
        assertThat(loader.hasChildren(loader.getINodeFromPath("/test1").getId())).isFalse();
        assertThatExceptionOfType(FileNotFoundException.class)
                .isThrownBy(() -> loader.hasChildren("/test3/nonexistent/path"));
    }

    @Test
    public void testLoadEmptyFSImage() throws IOException {
        try (RandomAccessFile file = new RandomAccessFile("src/test/resources/fsimage_0000000000000000000", "r")) {
            final FSImageLoader fsImageLoader = FSImageLoader.load(file);
            fsImageLoader.visit(new FsVisitor() {
                @Override
                public void onFile(FsImageProto.INodeSection.INode inode, String path) {
                    // Nothing
                }

                @Override
                public void onDirectory(FsImageProto.INodeSection.INode inode, String path) {
                    // Nothing
                }

                @Override
                public void onSymLink(FsImageProto.INodeSection.INode inode, String path) {
                    // Nothing
                }
            });
        }
    }

}
