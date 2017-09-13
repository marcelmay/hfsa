package de.m3y.hadoop.hdfs.hfsa.core;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

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
        RandomAccessFile file = new RandomAccessFile("src/test/resources/fsi_small.img", "r");
        loader = FSImageLoader.load(file);
    }

    @Test
    public void testLoadAndVisit() throws IOException {
        Set<String> paths = new HashSet<>();
        Set<String> files = new HashSet<>();

        loader.visit(new FsVisitor() {
            @Override
            public void onFile(FsImageProto.INodeSection.INode inode, String path) {
                final String fileName = ("/".equals(path) ? path : path + '/') + inode.getName().toStringUtf8();
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
                final String dirName = ("/".equals(path) ? path : path + '/') + inode.getName().toStringUtf8();
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
        });

        assertEquals(3, userNames.size());
        assertEquals(3, groupNames.size());
        assertEquals(14, sumDirs);
        assertEquals(16, sumFiles);
        assertEquals(0, sumSymLinks);
        assertEquals(356409344L, sumSize);

        Set<String> expectedPaths = new HashSet<>(Arrays.asList(
                "/", "/test1", "/test2", "/test3", "/test3/foo", "/test3/foo/bar", "/user", "/user/mm",
                "/datalake", "/datalake/asset1", "/datalake/asset2",
                "/datalake/asset3", "/datalake/asset3/subasset1", "/datalake/asset3/subasset2"));
        assertTrue(paths.containsAll(expectedPaths));
        assertEquals(expectedPaths.size(), paths.size());

        Set<String> expectedFiles = new HashSet<>(Arrays.asList(
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
        ));
        assertEquals(expectedFiles.size(), files.size());
        assertTrue(expectedFiles.containsAll(files));

        assertEquals(1, loader.getINodeFromPath("/test3/foo/bar/test_40MiB.img").getFile().getReplication());
        assertEquals(3, loader.getINodeFromPath("/test3/foo/bar/test_80MiB.img").getFile().getReplication());
        assertEquals(5, loader.getINodeFromPath("/test3/foo/bar/test_4MiB.img").getFile().getReplication());
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

        assertEquals(3, userNames.size());
        assertEquals(3, groupNames.size());
        assertEquals(3, sumDirs);
        assertEquals(10, sumFiles);
        assertEquals(0, sumSymLinks);
        assertEquals(348017664L, sumSize);

        Set<String> expectedPaths = new HashSet<>(Arrays.asList(
                "/test3", "/test3/foo", "/test3/foo/bar"));
        assertTrue(paths.containsAll(expectedPaths));
        assertEquals(expectedPaths.size(), paths.size());

        Set<String> expectedFiles = new HashSet<>(Arrays.asList(
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
        ));
        assertEquals(expectedFiles.size(), files.size());
        assertTrue(expectedFiles.containsAll(files));
//        loader.getINodeFromPath("")
    }

    @Test
    public void testGetInodeFromPath() throws IOException {
        final FsImageProto.INodeSection.INode rootNode = loader.getINodeFromPath("/");
        // Root node has empty name
        assertEquals("", rootNode.getName().toStringUtf8());
        assertEquals(FsImageProto.INodeSection.INode.Type.DIRECTORY, rootNode.getType());

        final FsImageProto.INodeSection.INode test3Node = loader.getINodeFromPath("/test3");
        assertEquals("test3", test3Node.getName().toStringUtf8());
        assertEquals(FsImageProto.INodeSection.INode.Type.DIRECTORY, test3Node.getType());

        final FsImageProto.INodeSection.INode test3FooBarNode = loader.getINodeFromPath("/test3/foo/bar");
        assertEquals("bar", test3FooBarNode.getName().toStringUtf8());
        assertEquals(FsImageProto.INodeSection.INode.Type.DIRECTORY, test3FooBarNode.getType());

        final FsImageProto.INodeSection.INode fileNode = loader.getINodeFromPath("/test3/test_160MiB.img");
        assertEquals("test_160MiB.img", fileNode.getName().toStringUtf8());
        assertEquals(FsImageProto.INodeSection.INode.Type.FILE, fileNode.getType());
    }

    @Test
    public void testGetChildPaths() throws IOException {
        List<String> childPaths = loader.getChildPaths("/");
        List<String> expectedChildPaths = Arrays.asList("/user", "/test1", "/test2", "/test3", "/datalake");
        assertTrue(expectedChildPaths.containsAll(childPaths));
        assertTrue(childPaths.containsAll(expectedChildPaths));
    }

    @Test
    public void testGetFileINodesInDirectory() throws IOException {
        // Directory with no files but another directory
        List<FsImageProto.INodeSection.INode> files = loader.getFileINodesInDirectory("/user");
        assertEquals(0, files.size());

        // Directory with two files
        files = loader.getFileINodesInDirectory("/test3");
        assertEquals(2, files.size());
        final List<String> fileNames = files.stream().map((n) -> n.getName().toStringUtf8()).collect(Collectors.toList());
        assertTrue(fileNames.contains("test.img"));
        assertTrue(fileNames.contains("test_160MiB.img"));

        // Root has a single file
        files = loader.getFileINodesInDirectory("/");
        assertEquals(1, files.size());

        // Invalid directory
        try {
            loader.getFileINodesInDirectory("/does-not-exist");
            fail("Expected exception");
        } catch (FileNotFoundException e) {
            // ok
        }

        // Invalid directory : path is file
        try {
            loader.getFileINodesInDirectory("/test3/test.img");
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void testHasINode() throws IOException {
        assertTrue(loader.hasINode("/user"));
        assertTrue(loader.hasINode("/test3/test.img"));
        assertFalse(loader.hasINode("/does-not-exist"));
        try {
            assertFalse(loader.hasINode("invalid-path"));
            fail("Expected exception for invalid path");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    @Test
    public void testHasChildren() throws IOException {
        assertTrue(loader.hasChildren("/"));
        assertTrue(loader.hasChildren(loader.getINodeFromPath("/").getId()));
        assertTrue(loader.hasChildren("/user"));
        assertTrue(loader.hasChildren(loader.getINodeFromPath("/user").getId()));
        assertTrue(loader.hasChildren("/test3/foo/bar/"));
        assertTrue(loader.hasChildren(loader.getINodeFromPath("/test3/foo/bar/").getId()));
        assertTrue(loader.hasChildren("/test3/foo/bar"));
        assertTrue(loader.hasChildren(loader.getINodeFromPath("/test3/foo/bar").getId()));
        assertFalse(loader.hasChildren("/test1"));
        assertFalse(loader.hasChildren(loader.getINodeFromPath("/test1").getId()));
        try {
            assertFalse(loader.hasChildren("/test3/nonexistent/path"));
            fail("Expected FileNotFoundException");
        } catch (FileNotFoundException e) {
            // Expected
        }
    }

}
