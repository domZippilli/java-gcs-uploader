package com.google.solutions.gcsuploader;

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

        LinkedList<Future<?>> results = new LinkedList<Future<?>>();

        for (String file : files) {
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

        // no new work for the nannies
        UploadNanny.shutdown();

    }
}