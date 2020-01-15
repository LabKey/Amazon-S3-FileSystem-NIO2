package com.upplication.s3fs.attribute;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;


public class S3BasicFileAttributeView implements BasicFileAttributeView {

    /**
     * S3 doesn't let you set an object's modified time, to match the source file, for example. Thus, we
     * use custom attributes to stash what we want to treat at the "real" last modified and other timestamps.
     */
    public static final String LABKEY_LAST_MODIFIED = "labkey-last-modified";
    public static final String LABKEY_LAST_ACCESS = "labkey-last-access";
    public static final String LABKEY_CREATE_TIME = "labkey-create-time";

    private S3Path s3Path;
    private Path nioPath;

    public S3BasicFileAttributeView(S3Path s3Path, Path nioPath) {
        this.s3Path = s3Path;
        this.nioPath = nioPath;
    }

    @Override
    public String name() {
        return "basic";
    }

    @Override
    public BasicFileAttributes readAttributes() throws IOException {
        return s3Path.getFileSystem().provider().readAttributes(s3Path, BasicFileAttributes.class);
    }

    @Override
    public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
        AmazonS3 client = s3Path.getFileStore().getFileSystem().getClient();
        String sourceBucketName = s3Path.getFileStore().getBucket().getName();
        String targetBucketName = sourceBucketName;
        String sourceKey = s3Path.getKey();
        String targetKey = sourceKey;
        try {
            ObjectMetadata metadataCopy = client.getObjectMetadata(sourceBucketName, sourceKey).clone();
            setMetadataTimes(metadataCopy, lastModifiedTime, lastAccessTime, createTime);

            // S3 doesn't let you modify attributes of existing objects. However, you can set them as part of a copy
            // operation, and the copy's source and target locations can be identical. This means we don't have to download
            // and reupload the object, which is nice.
            S3FileSystemProvider.copy(client, metadataCopy, sourceBucketName, sourceKey, targetBucketName, targetKey, nioPath);
        }
        catch (AmazonS3Exception e) {
            S3FileSystemProvider.translateAndThrowS3Exception(e, nioPath);
        }
    }


    public static ObjectMetadata setMetadataTimes(ObjectMetadata metadata, FileTime lastModified, FileTime lastAccess, FileTime createTime)
    {
        // Overwrite the current metadata with the arguments
        if (lastModified != null) {
            metadata.addUserMetadata(LABKEY_LAST_MODIFIED, Long.toString(lastModified.toMillis()));
        }
        if (lastAccess != null) {
            metadata.addUserMetadata(LABKEY_LAST_ACCESS, Long.toString(lastAccess.toMillis()));
        }
        if (createTime != null) {
            metadata.addUserMetadata(LABKEY_CREATE_TIME, Long.toString(createTime.toMillis()));
        }
        return metadata;
    }
}
