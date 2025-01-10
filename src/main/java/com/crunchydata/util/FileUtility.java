package com.crunchydata.util;

import java.io.File;

public class FileUtility {

    public static Boolean FileExistsCheck(String fileName) {

        // Create a File object
        File file = new File(fileName);

        // Check if the file exists
        return file.exists();

    }

}
