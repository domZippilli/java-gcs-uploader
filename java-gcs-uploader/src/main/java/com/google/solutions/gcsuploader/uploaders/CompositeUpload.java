/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.solutions.gcsuploader.uploaders;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Storage.ComposeRequest;
import com.google.common.io.ByteStreams;

import com.google.solutions.gcsuploader.Constants;

/**
 * CompositeUpload
 */
public class CompositeUpload implements Uploader {

    public String bucketName = null;
    public String fileName = null;
    public Boolean success = false;
    public BlobInfo blobInfo = null;
    public String crc32c = null;
    private ThreadPoolExecutor executor = null;
    private static Storage storage = null;

    static {
        storage = StorageOptions.getDefaultInstance().getService();
    }

    public CompositeUpload(String bucketName, String fileName, ThreadPoolExecutor executor) {
        this.bucketName = bucketName;
        this.fileName = fileName;
        this.executor = executor;
    }

    private BlobInfo createBlobInfo(String key) {
        BlobId blobId = BlobId.of(this.bucketName, key);
        BlobInfo.Builder builder = BlobInfo.newBuilder(blobId);
        return builder.build();
    }

    public String getCrc32c() {
        return this.crc32c;
    }

    private void print(String message) {
        System.out.println(this.fileName + ": " + message);
    }

    public void run() {
        File inputFile = new File(this.fileName);

        // first decide how many slices to upload, maxing out at 8
        long minimumSliceBytes = Constants.SLICED_THRESHOLD;
        int sliceCount = (int) Math.min(Constants.MAX_SLICES, Math.ceil(inputFile.length() / minimumSliceBytes));
        int sliceBytes = (int) inputFile.length() / sliceCount;
        LinkedList<BlobInfo> slices = new LinkedList<BlobInfo>();
        int idx = 0;
        Path inputPath = FileSystems.getDefault().getPath(inputFile.getPath());

        // save results
        LinkedList<Future<?>> results = new LinkedList<Future<?>>();

        print("Slicing for composite upload.");
        while (idx < sliceCount) {
            // create and store the chunk for later composition
            BlobInfo chunkBlob = createBlobInfo(inputFile.toString() + "_chunk_" + idx);
            slices.add(chunkBlob);

            int start = idx * sliceBytes;
            // limit to chunk end, unless final slice
            int limit = -1;
            if (idx < sliceCount - 1) {
                limit = sliceBytes;
            }

            // submit async slice upload
            AsyncBlobUpload uploader = new AsyncBlobUpload(inputPath, chunkBlob, start, limit);
            results.add(executor.submit(uploader));
            idx++;
        }

        print("Waiting for slices to complete.");
        for (Future<?> result : results) {
            try {
                result.get();
            } catch (InterruptedException | ExecutionException e) {
                System.err.println("Error writing slice of " + this.fileName);
                e.printStackTrace();
            }
        }

        print("Composing.");
        ComposeRequest.Builder finalCompose = ComposeRequest.newBuilder();
        // final target is a blob the same as the filename passed in
        finalCompose.setTarget(createBlobInfo(this.fileName));
        for (BlobInfo slice : slices) {
            finalCompose.addSource(slice.getName());
        }
        Blob finalBlob = storage.compose(finalCompose.build());

        print("Deleting slices.");
        for (BlobInfo slice : slices) {
            storage.delete(slice.getBlobId());
        }

        print("Composite upload complete.");

        this.blobInfo = finalBlob;
        this.crc32c = storage.get(this.blobInfo.getBlobId()).getCrc32c();
        this.success = true;
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
            System.out.println(inputPath.toString() + ": Uploading slice bytes " + this.start + "->"
                    + (this.limit > -1 ? this.start + this.limit - 1 : "end") + " to " + chunkBlob.getName() + ".");
            try (WriteChannel writer = storage.writer(chunkBlob);
                    InputStream is = Files.newInputStream(inputPath);
                    OutputStream os = Channels.newOutputStream(writer);) {
                writer.setChunkSize(Constants.CHUNK_SIZE);
                // skip to chunk start
                ByteStreams.skipFully(is, this.start);
                // limit to chunk end, unless -1
                if (limit > -1) {
                    ByteStreams.copy(ByteStreams.limit(is, limit), os);
                } else {
                    ByteStreams.copy(is, os);
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Error while uploading slice " + chunkBlob.getName());
            }
        }
    }
}
