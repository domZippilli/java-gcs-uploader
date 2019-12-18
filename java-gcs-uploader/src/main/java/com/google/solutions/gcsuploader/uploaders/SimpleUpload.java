package com.google.solutions.gcsuploader.uploaders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.io.ByteStreams;

import com.google.solutions.gcsuploader.Constants;

/**
 * SimpleUpload
 */
public class SimpleUpload implements Uploader {

    public String bucketName = null;
    public String fileName = null;
    public Boolean success = false;
    public BlobInfo blobInfo = null;
    public String crc32c = null;
    private static Storage storage = null;

    static {
        storage = StorageOptions.getDefaultInstance().getService();
    }

    public SimpleUpload(String bucketName, String fileName){
        this.bucketName = bucketName;
        this.fileName = fileName;
    }

    public void run() {
        Path inputPath = FileSystems.getDefault().getPath(this.fileName);
        BlobInfo blobInfo = createBlobInfo(this.fileName);
        
        try (WriteChannel writer = storage.writer(blobInfo);
                OutputStream os = Channels.newOutputStream(writer);
                InputStream is = Files.newInputStream(inputPath)) {
            writer.setChunkSize(Constants.CHUNK_SIZE);
            ByteStreams.copy(is, os);
        } catch (IOException e) {
            throw new RuntimeException("Error while loading " + fileName + " to GCS", e);
        }
        this.blobInfo = blobInfo;
        this.crc32c = storage.get(blobInfo.getBlobId()).getCrc32c();
        this.success = true;
    }

    private BlobInfo createBlobInfo(String key) {
        BlobId blobId = BlobId.of(this.bucketName, key);
        BlobInfo.Builder builder = BlobInfo.newBuilder(blobId);
        return builder.build();
    }

    public String getCrc32c() {
        return this.crc32c;
    }
}