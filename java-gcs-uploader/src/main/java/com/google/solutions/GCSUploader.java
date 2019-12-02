package com.google.solutions;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.io.ByteStreams;

/**
 * Test uploading a file to GCS.
 *
 */
public class GCSUploader {
    private static String bucketName = null;
    private static Storage storage = null;

    static {
        storage = StorageOptions.getDefaultInstance().getService();
    }

    public static void main(String[] args) {
        // Quick-and-dirty argument parsing.
        if (args.length != 2) {
            System.out.println("Arguments: BUCKET_NAME LOCAL_FILE");
            System.out.println("Local file path will be used for the object name in the bucket.");
            System.exit(1);
        }
        bucketName = args[0];
        String fileName = args[1];
        Path filePath = FileSystems.getDefault().getPath(fileName);

        // Calculate file size pre-flight
        File testFile = new File(fileName);
        long bytes = testFile.length();
        float gigabytes = bytes / 1000 / 1000 / 1000;

        // Perform the upload.
        System.out.println("Starting upload of GB: " + gigabytes);
        Instant start = Instant.now();
        doUpload(fileName, filePath);
        Instant finish = Instant.now();
        System.out.println("Completed upload.");

        // Compute and report statistics.
        Duration duration = Duration.between(start, finish);
        System.out.println("Elapsed time " + duration.toString());
        float bytesPerSecond = bytes / duration.getSeconds();
        float megabytesPerSecond = bytesPerSecond / 1000 / 1000;
        System.out.println("Effective MB/s: " + megabytesPerSecond);
        float megabitsPerSecond = (bytesPerSecond * 8) / 1000 / 1000;
        System.out.println("Average Mb/s: " + megabitsPerSecond);
    }

    private static BlobId doUpload(String key, Path localFilePath) {
        BlobInfo blobInfo = createBlobInfo(key);
        WriteChannel writer = storage.writer(blobInfo);
        writer.setChunkSize(100 * 1024 * 1024);

        try (OutputStream os = Channels.newOutputStream(writer);
                InputStream is = Files.newInputStream(localFilePath)) {
            ByteStreams.copy(is, os);
        } catch (IOException e) {
            throw new RuntimeException("Error while loading " + key + " to GCS", e);
        }

        return blobInfo.getBlobId();
    }

    private static BlobInfo createBlobInfo(String key) {
        BlobId blobId = BlobId.of(bucketName, key);
        BlobInfo.Builder builder = BlobInfo.newBuilder(blobId);
        return builder.build();
    }

}
