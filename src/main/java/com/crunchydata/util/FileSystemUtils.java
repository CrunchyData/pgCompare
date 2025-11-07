/*
 * Copyright 2012-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
