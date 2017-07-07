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

import static org.junit.Assert.*;

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

        loader.visit(new FsVisitor() {
            @Override
            public void onFile(FsImageProto.INodeSection.INode inode, String path) {
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
        assertEquals(7, sumDirs);
        assertEquals(10, sumFiles);
        assertEquals(0, sumSymLinks);
        assertEquals(348017664L, sumSize);
        Set<String> expectedPaths = new HashSet<>(Arrays.asList(
                "/test1", "/test2", "/test3", "/test3/foo", "/test3/foo/bar", "/user", "/user/mm"));
        assertTrue(paths.containsAll(expectedPaths));
        assertTrue(expectedPaths.containsAll(paths));
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
        // Empty directory
        List<FsImageProto.INodeSection.INode> files = loader.getFileINodesInDirectory("/");
        assertEquals(0, files.size());

        // Directory with two files
        files = loader.getFileINodesInDirectory("/test3");
        assertEquals(2, files.size());
        final List<String> fileNames = files.stream().map((n) -> n.getName().toStringUtf8()).collect(Collectors.toList());
        assertTrue(fileNames.contains("test.img"));
        assertTrue(fileNames.contains("test_160MiB.img"));

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
    public void testLoadAndVisitWithPath() throws IOException {
        Set<String> paths = new HashSet<>();

        loader.visit(new FsVisitor() {
            @Override
            public void onFile(FsImageProto.INodeSection.INode inode, String path) {
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
        }, "/test3");

        assertEquals(3, userNames.size());
        assertEquals(3, groupNames.size());
        assertEquals(2, sumDirs);
        assertEquals(10, sumFiles);
        assertEquals(0, sumSymLinks);
        assertEquals(348017664L, sumSize);
        Set<String> expectedPaths = new HashSet<>(Arrays.asList(
                "/test3", "/test3/foo", "/test3/foo/bar"));
        assertTrue(paths.containsAll(expectedPaths));
        assertTrue(expectedPaths.containsAll(paths));
    }

}
