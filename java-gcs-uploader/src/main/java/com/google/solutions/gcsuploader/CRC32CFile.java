package com.google.solutions.gcsuploader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;

/**
 * CRC32CFile
 */
public class CRC32CFile implements Runnable {

    private String fileName;
    public Boolean success = false;
    public String crc32c = null;

    public CRC32CFile(String fileName) {
        this.fileName = fileName;
    }

    public void run() {
        File target = new File(this.fileName);
        try (FileInputStream targetStream = new FileInputStream(target);) {
            ByteBuffer buf = ByteBuffer.allocate(10 * 1000 * 1000); //10MB
            Hasher hasher = Hashing.crc32c().newHasher();
            //InputStream targetStream = .openStream();
            while (targetStream.getChannel().read(buf) != -1) {
                buf.flip();
                hasher.putBytes(buf);
                buf.clear();
            }
            this.crc32c = BaseEncoding.base64()
                    .encode(Ints.toByteArray(hasher.hash().asInt()));
            this.success = true;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}