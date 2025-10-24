package com.crunchydata.util;

import java.io.File;

/**
 * Utility class for file operations.
 * Provides methods for file existence checking and file system operations.
 *
 * <p>This class handles basic file operations used throughout the application.</p>
 *
 * @author Brian Pace
 */
public class FileSystemUtils {

    // Private constructor to prevent instantiation
    private FileSystemUtils() {
        throw new UnsupportedOperationException("FileUtility is a utility class and cannot be instantiated.");
    }

    /**
     * Checks if a file exists at the specified path.
     *
     * @param fileName The path to the file to check
     * @return true if the file exists, false otherwise
     */
    public static Boolean FileExistsCheck(String fileName) {
        if (fileName == null || fileName.trim().isEmpty()) {
            return false;
        }
        
        File file = new File(fileName);
        return file.exists();
    }
}
