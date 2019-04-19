package com.bazaarvoice.awslocal.sqs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class TestUtils {
    public static File createTempDirectory() {
        try {
            final File directory = Files.createTempDirectory("sqs").toFile();
            directory.deleteOnExit(); // not sure if this works
            return directory;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
