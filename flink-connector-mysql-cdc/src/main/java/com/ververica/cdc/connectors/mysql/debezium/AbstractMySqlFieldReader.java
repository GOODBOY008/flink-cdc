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
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

/** Abstract class for decode MySQL return value according to different protocols. */
public abstract class AbstractMySqlFieldReader {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final MySqlConnectorConfig connectorConfig;

    protected AbstractMySqlFieldReader(MySqlConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public abstract Object readTimeField(ResultSet rs, int columnIndex) throws SQLException;

    public abstract Object readDateField(ResultSet rs, int columnIndex, Column column, Table table)
            throws SQLException;

    public abstract Object readTimestampField(
            ResultSet rs, int columnIndex, Column column, Table table) throws SQLException;

    protected void logInvalidValue(ResultSet resultSet, int columnIndex, Object value)
            throws SQLException {
        final String columnName = resultSet.getMetaData().getColumnName(columnIndex);
        logger.trace("Column '" + columnName + "', detected an invalid value of '" + value + "'");
    }
}
