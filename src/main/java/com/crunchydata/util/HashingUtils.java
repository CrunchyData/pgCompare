/*
 * Copyright 2012-2024 the original author or authors.
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
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class for hashing operations.
 * Provides methods to generate MD5 hash of a given input string.
 *
 * <p>This class is not instantiable.</p>
 *
 * @author Brian Pace
 */
public class HashingUtils {

    // Private constructor to prevent instantiation
    private HashingUtils() {
        throw new UnsupportedOperationException("HashUtility is a utility class and cannot be instantiated.");
    }

    // Constants for better maintainability
    private static final String MD5_ALGORITHM = "MD5";
    private static final String HEX_RADIX = "16";
    private static final String ZERO_PADDING = "0";
    private static final int MD5_HASH_LENGTH = 32;

    /**
     * Generates an MD5 hash for the given input string.
     *
     * @param input the input string to be hashed
     * @return the MD5 hash as a hexadecimal string
     * @throws RuntimeException if the MD5 algorithm is not available
     */
    public static String getMd5(String input) {
        if (input == null) {
            throw new IllegalArgumentException("Input cannot be null");
        }
        
        try {
            // Static getInstance method is called with hashing MD5
            MessageDigest md = MessageDigest.getInstance(MD5_ALGORITHM);

            // digest() method is called to calculate message digest
            // of an input digest() return array of byte
            byte[] messageDigest = md.digest(input.getBytes());

            // Convert byte array into signum representation
            BigInteger no = new BigInteger(1, messageDigest);

            // Convert message digest into hex value
            StringBuilder hashText = new StringBuilder(no.toString(Integer.parseInt(HEX_RADIX)));

            // Pad with leading zeros to ensure 32-character length
            while (hashText.length() < MD5_HASH_LENGTH) {
                hashText.insert(0, ZERO_PADDING);
            }
            return hashText.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not found", e);
        }
    }
}
