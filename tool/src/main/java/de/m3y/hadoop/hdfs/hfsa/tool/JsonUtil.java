package de.m3y.hadoop.hdfs.hfsa.tool;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import de.m3y.hadoop.hdfs.hfsa.core.FsImageData;
import de.m3y.hadoop.hdfs.hfsa.util.FsUtil;
import de.m3y.hadoop.hdfs.hfsa.util.SizeBucket;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;

/**
 * Helpers for generating JSON output.
 */
class JsonUtil {
    static class INodeTypeAdapter extends TypeAdapter<FsImageProto.INodeSection.INode> {
        private final FsImageData fsImageData;
        public INodeTypeAdapter(FsImageData fsImageData) {
            this.fsImageData = fsImageData;
        }

        @Override
        public void write(JsonWriter out, FsImageProto.INodeSection.INode value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                JsonWriter writer = out.beginObject()
                        .name("id").value(value.getId())
                    .name("name").value(value.getName().toStringUtf8());
                switch (value.getType()) {
                    case FILE -> {
                        FsImageProto.INodeSection.INodeFile file = value.getFile();
                        final PermissionStatus permissionStatus = fsImageData.getPermissionStatus(file.getPermission());
                        writer
                                .name("type").value("file")
                                .name("permission").value(FsUtil.toString(permissionStatus))
                                .name("mtime").value(file.getModificationTime())
                                .name("atime").value(file.getModificationTime())
                                .name("replication").value(file.getReplication())
                                .name("preferredBlockSize").value(file.getPreferredBlockSize())
                                .name("storagePolicyID").value(file.getStoragePolicyID())
                                .name("erasureCodingPolicyID").value(file.getErasureCodingPolicyID());
                        // TODO: Blocks,...
                    }
                    case DIRECTORY -> {
                        FsImageProto.INodeSection.INodeDirectory directory = value.getDirectory();
                        final PermissionStatus permissionStatus = fsImageData.getPermissionStatus(directory.getPermission());
                        writer
                                .name("type").value("directory")
                                .name("permission").value(FsUtil.toString(permissionStatus))
                                .name("nsQuota").value(directory.getNsQuota())
                                .name("dsQuota").value(directory.getDsQuota())
                                .name("mtime").value(directory.getModificationTime())
                                .name("atime").value(directory.getModificationTime());
                    }
                    case SYMLINK -> {
                        FsImageProto.INodeSection.INodeSymlink symlink = value.getSymlink();
                        final PermissionStatus permissionStatus = fsImageData.getPermissionStatus(symlink.getPermission());
                        writer
                                .name("type").value("symlink")
                                .name("permission").value(FsUtil.toString(permissionStatus))
                                .name("target").value(symlink.getTarget().toStringUtf8())
                                .name("mtime").value(symlink.getModificationTime())
                                .name("atime").value(symlink.getModificationTime());
                    }
                }
                writer.endObject();
            }
        }

        @Override
        public FsImageProto.INodeSection.INode read(JsonReader in) {
            throw new IllegalStateException("Not implemented/unused");
        }
    }

    static class LongAdderTypeAdapter extends TypeAdapter<LongAdder> {
        @Override
        public void write(JsonWriter out, LongAdder value) throws IOException {
            out.value(value.longValue());
        }

        @Override
        public LongAdder read(JsonReader in) {
            throw new IllegalStateException("Not implemented/unused");
        }
    }

    static class SizeBucketTypeAdapter extends TypeAdapter<SizeBucket> {
        @Override
        public void write(JsonWriter out, SizeBucket value) throws IOException {
            final String[] bucketUnits = FormatUtil.toStringSizeFormatted(value.computeBucketUpperBorders());
            long[] buckets = value.get();
            out.beginArray();
            for (int i = 0; i < bucketUnits.length; i++) {
                out.beginObject().name(bucketUnits[i]).value(buckets[i]).endObject();
            }
            out.endArray();
        }

        @Override
        public SizeBucket read(JsonReader in) {
            throw new IllegalStateException("Not implemented/unused");
        }
    }
}
