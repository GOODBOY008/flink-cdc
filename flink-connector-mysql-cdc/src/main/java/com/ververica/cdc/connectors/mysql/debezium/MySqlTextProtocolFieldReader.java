/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.debezium;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;

/** Decode text protocol value for MySQL. */
public class MySqlTextProtocolFieldReader extends AbstractMySqlFieldReader {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(MySqlTextProtocolFieldReader.class);

    public MySqlTextProtocolFieldReader(MySqlConnectorConfig config) {
        super(config);
    }

    /**
     * As MySQL connector/J implementation is broken for MySQL type "TIME" we have to use a
     * binary-ish workaround.
     *
     * @link https://issues.jboss.org/browse/DBZ-342
     */
    @Override
    public Object readTimeField(ResultSet rs, int columnIndex) throws SQLException {
        Blob b = rs.getBlob(columnIndex);
        if (b == null) {
            // Don't continue parsing time field if it is null
            return null;
        } else if (b.length() == 0) {
            LOGGER.warn("Encountered a zero length blob for column index {}", columnIndex);
            return null;
        }

        try {
            return MySqlValueConverters.stringToDuration(
                    new String(b.getBytes(1, (int) (b.length())), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            logInvalidValue(rs, columnIndex, b);
            logger.error(
                    "Could not read MySQL TIME value as UTF-8. "
                            + "Enable TRACE logging to log the problematic column and its value.");
            throw new RuntimeException(e);
        }
    }

    /**
     * In non-string mode the date field can contain zero in any of the date part which we need to
     * handle as all-zero.
     */
    @Override
    public Object readDateField(ResultSet rs, int columnIndex, Column column, Table table)
            throws SQLException {
        Blob b = rs.getBlob(columnIndex);
        if (b == null) {
            return null; // Don't continue parsing date field if it is null
        }

        try {
            return MySqlValueConverters.stringToLocalDate(
                    new String(b.getBytes(1, (int) (b.length())), "UTF-8"), column, table);
        } catch (UnsupportedEncodingException e) {
            logInvalidValue(rs, columnIndex, b);
            logger.error(
                    "Could not read MySQL DATE value as UTF-8. "
                            + "Enable TRACE logging to log the problematic column and its value.");
            throw new RuntimeException(e);
        }
    }

    /**
     * In non-string mode the time field can contain zero in any of the date part which we need to
     * handle as all-zero.
     */
    @Override
    public Object readTimestampField(ResultSet rs, int columnIndex, Column column, Table table)
            throws SQLException {
        Blob b = rs.getBlob(columnIndex);
        if (b == null) {
            return null; // Don't continue parsing timestamp field if it is null
        } else if (b.length() == 0) {
            LOGGER.warn("Encountered a zero length blob for column index {}", columnIndex);
            return null;
        }

        try {
            return MySqlValueConverters.containsZeroValuesInDatePart(
                            (new String(b.getBytes(1, (int) (b.length())), "UTF-8")), column, table)
                    ? null
                    : rs.getTimestamp(columnIndex, Calendar.getInstance());
        } catch (UnsupportedEncodingException e) {
            logInvalidValue(rs, columnIndex, b);
            logger.error(
                    "Could not read MySQL DATETIME value as UTF-8. "
                            + "Enable TRACE logging to log the problematic column and its value.");
            throw new RuntimeException(e);
        }
    }
}
