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

import static com.crunchydata.util.ColumnMetadataUtils.TIMESTAMP_TYPES;

/**
 * SQL for carrying temporal primary-key values through the pk JSON and back into the
 * check filter.
 *
 * <p>The pk JSON of a row is read back by check and bound to {@code col = ?} on BOTH the
 * source and the target (the repository picks one pk per pk_hash from either side). A
 * temporal key therefore has to be written as text that every platform parses the same
 * way, without losing precision, and without depending on session settings such as
 * NLS_DATE_FORMAT, SET LANGUAGE/DATEFORMAT or DateStyle. Before this class the raw column
 * went through the platform's replace(), which PostgreSQL rejects for temporal types, and
 * which other platforms ran through an implicit, session-dependent conversion (SQL Server
 * style 0 drops the seconds, Oracle NLS_DATE_FORMAT drops the time).</p>
 *
 * <p>Canonical key text: {@code YYYY-MM-DD HH24:MI:SS[.fraction]} for date and timestamp
 * types (date types carry {@code 00:00:00}) and {@code HH24:MI:SS[.fraction]} for time.
 * The fraction carries the platform's full precision (up to 9 digits); the filter trims it
 * to what the platform can parse. Values are normalized to UTC exactly where
 * {@link DataTypeCastingUtils#castTimestamp} normalizes them for the pk_hash, so a key
 * names the same instant the pairing used.</p>
 *
 * @author pgCompare
 */
public final class TemporalKeyUtils {

    private static final String UTC = "UTC";

    private TemporalKeyUtils() {
        throw new UnsupportedOperationException("TemporalKeyUtils is a utility class and cannot be instantiated.");
    }

    /**
     * Whether a primary-key column of this type is written with the canonical temporal
     * key text instead of the generic string path.
     *
     * @param platform Database platform
     * @param dataType Column data type as stored in the repository
     * @return true for date, datetime, timestamp and time types
     */
    public static boolean isTemporalKey(String platform, String dataType) {
        if (dataType == null) {
            return false;
        }
        String type = dataType.toLowerCase();
        if (!TIMESTAMP_TYPES.contains(type) || "year".equals(type)) {
            return false;
        }
        // SQL Server reports rowversion as 'timestamp'; it is binary, not temporal.
        return !("mssql".equals(platform) && "timestamp".equals(type));
    }

    /**
     * Expression that renders the column as canonical key text, to be embedded in the pk JSON.
     *
     * @param platform Database platform
     * @param dataType Column data type
     * @param column   Column name, already quoted if needed
     * @return SQL expression producing the key text
     */
    public static String keyText(String platform, String dataType, String column) {
        String type = dataType.toLowerCase();
        boolean isTime = "time".equals(type);
        boolean isDate = "date".equals(type);

        return switch (platform) {
            case "mssql" -> {
                if (isTime) {
                    yield String.format("convert(varchar(16), %s, 121)", column);
                } else if (isDate) {
                    yield String.format("convert(varchar(19), cast(%s as datetime2(0)), 121)", column);
                } else if ("datetimeoffset".equals(type)) {
                    yield String.format("convert(varchar(27), cast(switchoffset(%s, '+00:00') as datetime2(7)), 121)", column);
                }
                // Style 121 renders the type's own precision (3 digits for datetime, n for datetime2(n)).
                yield String.format("convert(varchar(27), %s, 121)", column);
            }
            case "oracle" -> {
                if (isDate) {
                    yield String.format("to_char(%s,'YYYY-MM-DD HH24:MI:SS')", column);
                } else if (hasTimeZone(platform, type)) {
                    yield String.format("to_char(%s at time zone '%s','YYYY-MM-DD HH24:MI:SS.FF9')", column, UTC);
                }
                yield String.format("to_char(%s,'YYYY-MM-DD HH24:MI:SS.FF9')", column);
            }
            case "mysql" -> {
                if (isTime) {
                    yield String.format("time_format(%s,'%%H:%%i:%%s.%%f')", column);
                } else if (hasTimeZone(platform, type)) {
                    yield String.format("date_format(convert_tz(%s,@@session.time_zone,'%s'),'%%Y-%%m-%%d %%H:%%i:%%s.%%f')", column, UTC);
                }
                yield String.format("date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s.%%f')", column);
            }
            case "mariadb" -> isTime
                    ? String.format("time_format(%s,'%%H:%%i:%%s.%%f')", column)
                    : String.format("date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s.%%f')", column);
            case "db2" -> {
                if (isTime) {
                    yield String.format("char(%s, JIS)", column);
                } else if (isDate) {
                    yield String.format("to_char(%s,'YYYY-MM-DD HH24:MI:SS')", column);
                }
                yield String.format("to_char(%s,'YYYY-MM-DD HH24:MI:SS.FF9')", column);
            }
            case "snowflake" -> {
                if (isTime) {
                    yield String.format("to_char(%s,'HH24:MI:SS.FF9')", column);
                } else if (isDate) {
                    yield String.format("to_char(cast(%s as timestamp_ntz),'YYYY-MM-DD HH24:MI:SS')", column);
                } else if (hasTimeZone(platform, type)) {
                    yield String.format("to_char(convert_timezone('%s', %s),'YYYY-MM-DD HH24:MI:SS.FF9')", UTC, column);
                }
                yield String.format("to_char(%s,'YYYY-MM-DD HH24:MI:SS.FF9')", column);
            }
            default -> {
                if (isTime) {
                    yield String.format("to_char(%s,'HH24:MI:SS.US')", column);
                } else if (hasTimeZone(platform, type)) {
                    yield String.format("to_char(%s at time zone '%s','YYYY-MM-DD HH24:MI:SS.US')", column, UTC);
                }
                yield String.format("to_char(%s,'YYYY-MM-DD HH24:MI:SS.US')", column);
            }
        };
    }

    /**
     * Expression that parses one bind parameter ({@code ?}) holding canonical key text
     * into a value comparable with the column, using only explicit formats or ISO input.
     *
     * <p>The value is converted to the column's own type where the platform would
     * otherwise compare in a wider type and miss the row (SQL Server compares datetime with
     * datetime2 after a precise conversion, so .003 no longer equals .003), and kept in the
     * column's type family elsewhere so the primary-key index is still used.</p>
     *
     * @param platform Database platform
     * @param dataType Column data type
     * @return SQL expression containing exactly one {@code ?}
     */
    public static String keyParameter(String platform, String dataType) {
        String type = dataType.toLowerCase();
        boolean isTime = "time".equals(type);
        boolean isDate = "date".equals(type);

        return switch (platform) {
            case "mssql" -> switch (type) {
                case "time" -> "cast(left(?, 16) as time)";
                case "datetimeoffset" -> "todatetimeoffset(convert(datetime2(7), left(?, 27), 121), '+00:00')";
                case "date" -> "cast(convert(datetime2(7), left(?, 27), 121) as date)";
                case "smalldatetime" -> "cast(convert(datetime2(7), left(?, 27), 121) as smalldatetime)";
                case "datetime" -> "cast(convert(datetime2(7), left(?, 27), 121) as datetime)";
                default -> "convert(datetime2(7), left(?, 27), 121)";
            };
            case "oracle" -> {
                if (isDate) {
                    yield "to_date(substr(?, 1, 19), 'YYYY-MM-DD HH24:MI:SS')";
                } else if (hasTimeZone(platform, type)) {
                    yield String.format("from_tz(to_timestamp(substr(?, 1, 29), 'YYYY-MM-DD HH24:MI:SS.FF9'), '%s')", UTC);
                }
                yield "to_timestamp(substr(?, 1, 29), 'YYYY-MM-DD HH24:MI:SS.FF9')";
            }
            case "mysql" -> {
                if (isTime) {
                    yield "cast(? as time(6))";
                } else if (isDate) {
                    yield "cast(? as date)";
                } else if (hasTimeZone(platform, type)) {
                    yield String.format("convert_tz(cast(? as datetime(6)),'%s',@@session.time_zone)", UTC);
                }
                yield "cast(? as datetime(6))";
            }
            case "mariadb" -> isTime ? "cast(? as time(6))" : isDate ? "cast(? as date)" : "cast(? as datetime(6))";
            case "db2" -> isTime ? "time(cast(? as varchar(40)))"
                    : isDate ? "date(cast(? as varchar(40)))"
                    : "timestamp(cast(? as varchar(40)))";
            case "snowflake" -> {
                if (isTime) {
                    yield "to_time(?)";
                } else if (isDate) {
                    yield "to_date(substr(?, 1, 10))";
                } else if (hasTimeZone(platform, type)) {
                    yield "to_timestamp_tz(? || ' +00:00')";
                }
                yield "to_timestamp_ntz(?)";
            }
            default -> {
                if (isTime) {
                    yield "cast(? as time)";
                } else if (isDate) {
                    yield "cast(? as date)";
                } else if (hasTimeZone(platform, type)) {
                    yield String.format("(cast(? as timestamp) at time zone '%s')", UTC);
                }
                yield "cast(? as timestamp)";
            }
        };
    }

    /**
     * Whether the platform normalizes this type to UTC, mirroring castTimestamp().
     */
    private static boolean hasTimeZone(String platform, String type) {
        boolean tzName = type.contains("time zone") || type.contains("tz");
        return switch (platform) {
            case "oracle", "db2" -> tzName;
            // castTimestamp converts every datetime/timestamp from the session time zone to UTC.
            case "mysql" -> type.contains("timestamp") || type.contains("datetime");
            case "mariadb" -> false;
            default -> tzName && !type.contains("ntz");
        };
    }
}
