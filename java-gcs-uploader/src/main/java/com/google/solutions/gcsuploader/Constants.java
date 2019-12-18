package com.google.solutions.gcsuploader;

public class Constants {
    public static final int CHUNK_SIZE = 15 * 1000 * 1000;
    public static final long SLICED_THRESHOLD = CHUNK_SIZE * 4;
    public static final int MAX_SLICES = 10;
    public static final int SIMULTANEOUS_FILES = Runtime.getRuntime().availableProcessors();
    public static final int UPLOAD_THREADS = Runtime.getRuntime().availableProcessors() * 4;
}