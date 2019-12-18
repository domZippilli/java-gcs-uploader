package com.google.solutions.gcsuploader;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Test uploading a file to GCS.
 *
 */
public class GCSUploader {

    private static ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors
            .newFixedThreadPool(Constants.SIMULTANEOUS_FILES);

    public static void main(String[] args) {

        Options options = new Options();

        Option bucketOption = new Option("b", "bucket", true, "Upload bucket target.");
        bucketOption.setRequired(true);
        options.addOption(bucketOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("As arguments, provide [OPTIONS] FILE1 FILE2...", options);
            System.exit(1);
            return;
        }

        String bucket = cmd.getOptionValue("bucket");

        String[] files = cmd.getArgs();
        if (files.length < 1) {
            System.out.println("No files provided.");
            System.exit(1);
        }

        print("Starting all uploads.");
        print("Chunk size is: " + Constants.CHUNK_SIZE);
        print("Simultaneous files (core count) is: " + Constants.SIMULTANEOUS_FILES);
        print("Upload threads is: " + Constants.UPLOAD_THREADS);
        Instant start = Instant.now();

        LinkedList<Future<?>> results = new LinkedList<Future<?>>();
        long bytes = 0;

        for (String file : files) {
            File toUpload = new File(file);
            if (!toUpload.exists()){
                print("Bad file, skipping: " + toUpload);
                continue;
            }
            bytes += toUpload.length();
            UploadNanny nannyWork = new UploadNanny(bucket, file);
            results.add(executor.submit(nannyWork));
        }

        // no new work
        executor.shutdown();

        for (Future<?> result : results) {
            try {
                result.get();
            } catch (InterruptedException | ExecutionException e) {
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

        // no new work for the nannies
        UploadNanny.shutdown();

    }

    private static void print(String message){
        System.out.println("main: " + message);
    }
}