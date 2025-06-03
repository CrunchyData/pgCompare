package com.crunchydata.services;

import java.sql.*;

public class dbDuck {

        /**
         * Loads a given ResultSet into a DuckDB table.
         *
         * @param rs          The source ResultSet.
         * @param duckDbConn  A valid connection to DuckDB.
         * @param targetTable The name of the target table to create and insert into.
         * @throws SQLException If an error occurs during DB operations.
         */
        public static void loadIntoDuckDB(ResultSet rs, Connection duckDbConn, String targetTable) throws SQLException {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            // Build CREATE TABLE SQL
            StringBuilder createSQL = new StringBuilder("CREATE TABLE ").append(targetTable).append(" (");
            for (int i = 1; i <= columnCount; i++) {
                createSQL.append(meta.getColumnLabel(i)).append(" ").append(mapSQLType(meta.getColumnType(i)));
                if (i < columnCount) {
                    createSQL.append(", ");
                }
            }
            createSQL.append(");");

            try (Statement stmt = duckDbConn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + targetTable);
                stmt.execute(createSQL.toString());
            }

            // Prepare INSERT INTO statement
            StringBuilder insertSQL = new StringBuilder("INSERT INTO ").append(targetTable).append(" VALUES (");
            insertSQL.append("?,".repeat(columnCount));
            insertSQL.setLength(insertSQL.length() - 1); // Remove last comma
            insertSQL.append(");");

            try (PreparedStatement ps = duckDbConn.prepareStatement(insertSQL.toString())) {
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        ps.setObject(i, rs.getObject(i));
                    }
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }

        // Map SQL types to DuckDB-compatible types
        private static String mapSQLType(int sqlType) {
            return switch (sqlType) {
                case Types.INTEGER -> "INTEGER";
                case Types.BIGINT -> "BIGINT";
                case Types.FLOAT, Types.REAL, Types.DOUBLE -> "DOUBLE";
                case Types.DECIMAL, Types.NUMERIC -> "DECIMAL";
                case Types.VARCHAR, Types.LONGVARCHAR, Types.CHAR -> "VARCHAR";
                case Types.BOOLEAN, Types.BIT -> "BOOLEAN";
                case Types.DATE -> "DATE";
                case Types.TIMESTAMP -> "TIMESTAMP";
                default -> "VARCHAR"; // Fallback for unsupported or unknown types
            };
    }
}
