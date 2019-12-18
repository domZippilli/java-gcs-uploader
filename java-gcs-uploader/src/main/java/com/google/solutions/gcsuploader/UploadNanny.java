package com.google.solutions.gcsuploader;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.solutions.gcsuploader.uploaders.CompositeUpload;
import com.google.solutions.gcsuploader.uploaders.SimpleUpload;
import com.google.solutions.gcsuploader.uploaders.Uploader;

/**
 * Test uploading a file to GCS.
 *
 */
public class UploadNanny implements Runnable {
    private String bucketName = null;
    private String fileName = null;
    private static ThreadPoolExecutor commonExecutor = (ThreadPoolExecutor) Executors
            .newFixedThreadPool(Constants.UPLOAD_THREADS);

    public UploadNanny(String bucketName, String fileName) {
        this.bucketName = bucketName;
        this.fileName = fileName;
    }

    public void run() {
        // Calculate file size pre-flight
        File testFile = new File(fileName);
        long bytes = testFile.length();
        float gigabytes = bytes / 1000 / 1000 / 1000;

        // Perform the upload.
        print("Starting upload of GB: " + gigabytes);
        print("Chunk size is: " + Constants.CHUNK_SIZE);
        Instant start = Instant.now();
        while (!doUpload()) {
            print("Upload error! Waiting and retrying.");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        Instant finish = Instant.now();
        print("Completed upload.");

        // Compute and report statistics.
        Duration duration = Duration.between(start, finish);
        print("Elapsed time " + duration.toString());
        float bytesPerSecond = bytes / duration.getSeconds();
        float megabytesPerSecond = bytesPerSecond / 1000 / 1000;
        print("Effective MB/s: " + megabytesPerSecond);
        float megabitsPerSecond = (bytesPerSecond * 8) / 1000 / 1000;
        print("Average Mb/s: " + megabitsPerSecond);
    }

    private Boolean doUpload() {
        // The job here is to decide how to upload, and check success.
        File inputFile = new File(this.fileName);

        // Form simple upload subtasks
        Uploader uploadWork;
        if (inputFile.length() < Constants.SLICED_THRESHOLD) {
            uploadWork = new SimpleUpload(this.bucketName, this.fileName);
        } else {
            uploadWork = new CompositeUpload(this.bucketName, this.fileName, commonExecutor);
        }
        Future<?> uploadResult = commonExecutor.submit(uploadWork);
        print("Started upload.");
        CRC32CFile checksumWork = new CRC32CFile(this.fileName);
        Future<?> checksumResult = commonExecutor.submit(checksumWork);
        print("Started checksum.");
        // Wait for subtasks to complete
        try {
            checksumResult.get();
            print("Completed checksum.");
            uploadResult.get();
            print("Completed upload.");
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Error writing chunk.");
            e.printStackTrace();
        }
        String blobChecksum = uploadWork.getCrc32c();
        String fileChecksum = checksumWork.crc32c;
        print("\n\tChecksum for blob: " + blobChecksum + "\n\tChecksum for file: " + fileChecksum);
        return fileChecksum.equals(blobChecksum);
    }

    private void print(String message) {
        System.out.println(this.fileName + ": " + message);
    }

    public static void shutdown() {
        commonExecutor.shutdown();
    }
}
