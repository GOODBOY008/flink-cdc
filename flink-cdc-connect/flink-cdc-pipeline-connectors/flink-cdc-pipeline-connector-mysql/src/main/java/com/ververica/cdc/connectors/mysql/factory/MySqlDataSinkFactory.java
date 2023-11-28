package com.ververica.cdc.connectors.mysql.factory;

import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.factories.DataSinkFactory;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.connectors.mysql.sink.MySqlDataSink;
import com.ververica.cdc.connectors.mysql.sink.MysqlOptions;

import java.util.HashSet;
import java.util.Set;

/** A factory to create {@link DataSink} for MySQL. */
public class MySqlDataSinkFactory implements DataSinkFactory {

    @Override
    public DataSink createDataSink(Context context) {
     return new MySqlDataSink(context);
    }

    @Override
    public String identifier() {
        return "mysql-sink";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MysqlOptions.URL);
        options.add(MysqlOptions.USERNAME);
        options.add(MysqlOptions.PASSWORD);
        options.add(MysqlOptions.DRIVER);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MysqlOptions.MAX_RETRY_TIMEOUT);
        options.add(MysqlOptions.SINK_BUFFER_FLUSH_MAX_ROWS);
        options.add(MysqlOptions.SINK_BUFFER_FLUSH_INTERVAL);
        options.add(MysqlOptions.SINK_MAX_RETRIES);
        return options;
    }
}
