package com.google.solutions.gcsuploader.uploaders;

/**
 * Uploader
 */
public interface Uploader extends Runnable{

    public String getCrc32c();
    
}