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
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.ComposeRequest;
import com.google.cloud.storage.StorageOptions;
import com.google.common.io.ByteStreams;

/**
 * Test uploading a file to GCS.
 *
 */
public class GCSUploader {
    private static String bucketName = null;
    private static Storage storage = null;
    private static int CHUNK_SIZE = 15 * 1000 * 1000;
    private static long SLICED_THRESHOLD = CHUNK_SIZE * 4;
    private static int MAX_SLICES = 8;
    private static int THREADS = Runtime.getRuntime().availableProcessors() * 2;
    // recommend making this static as having more than one would make tuning difficult, likely too many threads.
    private static ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(THREADS);

    static {
        storage = StorageOptions.getDefaultInstance().getService();
    }

    public static void main(String[] args) {
        // Quick-and-dirty argument parsing.
        if (args.length < 2) {
            System.out.println("Arguments: BUCKET_NAME LOCAL_FILE [CHUNK_SIZE_MB]");
            System.out.println("Local file path will be used for the object name in the bucket.");
            System.exit(1);
        }
        bucketName = args[0];
        String fileName = args[1];
        if (args.length >= 3) {
            int newChunkSize = Integer.parseInt(args[2]) * 1000 * 1000;
            System.out.println("Using user-specified chunk size.");
            CHUNK_SIZE = newChunkSize;
        }

        // Calculate file size pre-flight
        File testFile = new File(fileName);
        long bytes = testFile.length();
        float gigabytes = bytes / 1000 / 1000 / 1000;

        // Perform the upload.
        System.out.println("Starting upload of GB: " + gigabytes);
        System.out.println("Chunk size is: " + CHUNK_SIZE);
        Instant start = Instant.now();
        doUpload(fileName);
        Instant finish = Instant.now();
        System.out.println("Completed upload.");

        // no new tasks
        executor.shutdown();


        // Compute and report statistics.
        Duration duration = Duration.between(start, finish);
        System.out.println("Elapsed time " + duration.toString());
        float bytesPerSecond = bytes / duration.getSeconds();
        float megabytesPerSecond = bytesPerSecond / 1000 / 1000;
        System.out.println("Effective MB/s: " + megabytesPerSecond);
        float megabitsPerSecond = (bytesPerSecond * 8) / 1000 / 1000;
        System.out.println("Average Mb/s: " + megabitsPerSecond);
    }

    private static BlobId doUpload(String fileName) {
        BlobInfo blobInfo = createBlobInfo(fileName);
        File inputFile = new File(fileName);

        if (inputFile.length() > SLICED_THRESHOLD) {
            doCompositeUpload(inputFile, blobInfo);
        } else {
            Path inputPath = FileSystems.getDefault().getPath(fileName);
            try (WriteChannel writer = storage.writer(blobInfo);
                    OutputStream os = Channels.newOutputStream(writer);
                    InputStream is = Files.newInputStream(inputPath)) {
                writer.setChunkSize(CHUNK_SIZE);
                ByteStreams.copy(is, os);
            } catch (IOException e) {
                throw new RuntimeException("Error while loading " + fileName + " to GCS", e);
            }
        }
        return blobInfo.getBlobId();
    }

    private static BlobInfo createBlobInfo(String key) {
        BlobId blobId = BlobId.of(bucketName, key);
        BlobInfo.Builder builder = BlobInfo.newBuilder(blobId);
        return builder.build();
    }

    private static BlobId doCompositeUpload(File inputFile, BlobInfo compositeBlob) {
        // first decide how many slices to upload, maxing out at 8
        long minimumSliceBytes = SLICED_THRESHOLD;
        int sliceCount = (int) Math.min(MAX_SLICES, Math.ceil(inputFile.length() / minimumSliceBytes));
        int sliceBytes = (int) inputFile.length() / sliceCount;
        LinkedList<BlobInfo> slices = new LinkedList<BlobInfo>();
        int idx = 0;
        Path inputPath = FileSystems.getDefault().getPath(inputFile.getName());

        // save results
        LinkedList<Future<?>> results = new LinkedList<Future<?>>();

        System.out.println(inputFile.getName() + ": Slicing for composite upload.");
        while (idx < sliceCount) {
            // create and store the chunk for later composition
            BlobInfo chunkBlob = createBlobInfo(inputFile.getName() + "_chunk_" + idx);
            slices.add(chunkBlob);

            int start = idx * sliceBytes;
            // limit to chunk end, unless final slice
            int limit = -1;
            if (idx < sliceCount - 1) {
                limit = sliceBytes - 1;
            }

            // submit async slice upload
            AsyncBlobUpload uploader = new AsyncBlobUpload(inputPath, chunkBlob, start, limit);
            results.add(executor.submit(uploader));
            idx++;
        }

        System.out.println(inputFile.getName() + ": Waiting for slices to complete.");
        for (Future<?> result : results) {
            try {
                result.get();
            } catch (InterruptedException | ExecutionException e) {
                System.err.println("Error writing chunk.");
                e.printStackTrace();
            }
        }

        System.out.println(inputFile.getName() + ": Composing.");
        ComposeRequest.Builder finalCompose = ComposeRequest.newBuilder();
        finalCompose.setTarget(compositeBlob);
        for (BlobInfo slice : slices) {
            finalCompose.addSource(slice.getName());
        }
        Blob finalBlob = storage.compose(finalCompose.build());

        System.out.println(inputFile.getName() + ": Deleting slices.");
        for (BlobInfo slice : slices) {
            storage.delete(slice.getBlobId());
        }

        System.out.println(inputFile.getName() + ": Composite upload complete.");
        return finalBlob.getBlobId();
    }

    private static class AsyncBlobUpload implements Runnable {

        private final Path inputPath;
        private final BlobInfo chunkBlob;
        private final int start;
        private final int limit;

        public AsyncBlobUpload(Path inputPath, BlobInfo chunkBlob, int start, int limit) {
            this.inputPath = inputPath;
            this.chunkBlob = chunkBlob;
            this.start = start;
            this.limit = limit;
        }

        @Override
        public void run() {
            System.out.println(inputPath.getFileName() + ": Uploading slice bytes " + this.start + " to "
                    + (this.limit > -1 ? this.start + this.limit : "end."));
            try (WriteChannel writer = storage.writer(chunkBlob);
                    InputStream is = Files.newInputStream(inputPath);
                    OutputStream os = Channels.newOutputStream(writer);) {
                writer.setChunkSize(CHUNK_SIZE);
                // skip to chunk start
                ByteStreams.skipFully(is, this.start);
                // limit to chunk end, unless -1
                InputStream slice = is;
                if (limit > -1) {
                    slice = ByteStreams.limit(is, limit);
                }
                // copy chunk
                ByteStreams.copy(slice, os);
                slice.close();
            } catch (IOException e) {
                throw new RuntimeException("Error while copying " + this.inputPath.getFileName());
            }
        }
    }
}
