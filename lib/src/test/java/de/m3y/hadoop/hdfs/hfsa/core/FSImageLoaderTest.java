package de.m3y.hadoop.hdfs.hfsa.core;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * fsi_small.img test data content:
 * <pre>
 * mm   supergroup          0  /test1
 * mm   supergroup          0  /test2
 * mm   supergroup          0  /test3
 * mm   supergroup          0  /test3/foo
 * mm   supergroup          0  /test3/foo/bar
 * mm   nobody       20971520  /test3/foo/bar/test_20MiB.img
 * mm   supergroup    2097152  /test3/foo/bar/test_2MiB.img
 * mm   supergroup   41943040  /test3/foo/bar/test_40MiB.img
 * mm   supergroup    4145152  /test3/foo/bar/test_4MiB.img
 * mm   supergroup    5181440  /test3/foo/bar/test_5MiB.img
 * mm   supergroup   83886080  /test3/foo/bar/test_80MiB.img
 * root root             1024  /test3/foo/test_1KiB.img
 * mm   supergroup   20971520  /test3/foo/test_20MiB.img
 * mm   supergroup    1048576  /test3/test.img
 * foo  nobody      167772160  /test3/test_160MiB.img
 * mm   supergroup       2048  /test_2KiB.img
 * mm   supergroup          0  /user
 * mm   supergroup          0  /user/mm
 * </pre>
 */
public class FSImageLoaderTest {
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
        });

        assertEquals(3, userNames.size());
        assertEquals(3, groupNames.size());
        assertEquals(8, sumDirs);
        assertEquals(11, sumFiles);
        assertEquals(0, sumSymLinks);
        assertEquals(348019712L, sumSize);

        Set<String> expectedPaths = new HashSet<>(Arrays.asList(
                "/", "/test1", "/test2", "/test3", "/test3/foo", "/test3/foo/bar", "/user", "/user/mm"));
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
                "/test3/foo/bar/test_80MiB.img"
        ));
        assertEquals(expectedFiles.size(),files.size());
        assertTrue(expectedFiles.containsAll(files));
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
                System.out.println(path + " : " + inode.getName().toStringUtf8());
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
        },"/test3");

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
        assertEquals(expectedFiles.size(),files.size());
        assertTrue(expectedFiles.containsAll(files));
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
        List<String> expectedChildPaths = Arrays.asList("/user", "/test1", "/test2", "/test3");
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

}
