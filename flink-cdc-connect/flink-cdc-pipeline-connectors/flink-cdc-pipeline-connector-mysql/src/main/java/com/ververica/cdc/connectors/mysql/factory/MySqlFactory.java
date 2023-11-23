package com.ververica.cdc.connectors.mysql.factory;

import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.factories.DataSinkFactory;
import com.ververica.cdc.common.factories.DataSourceFactory;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.common.source.DataSource;

import java.util.Set;

/** A factory to create {@link DataSource} and {@link DataSink} for MySQL. */
public class MySqlFactory implements DataSourceFactory, DataSinkFactory {

  public static final String MYSQL_IDENTIFIER = "mysql";

  @Override
  public DataSink createDataSink(Context context) {
    return null;
  }

  @Override
  public DataSource createDataSource(Context context) {
    return null;
  }

  @Override
  public String identifier() {
    return MYSQL_IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return null;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return null;
  }
}
