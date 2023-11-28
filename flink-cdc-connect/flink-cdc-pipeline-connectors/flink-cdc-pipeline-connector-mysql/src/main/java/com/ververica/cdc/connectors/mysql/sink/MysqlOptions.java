package com.ververica.cdc.connectors.mysql.sink;

import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.ConfigOptions;

import java.time.Duration;

/** Options for the Flink jdbc sink. */
public class MysqlOptions {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC database URL.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC user name.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC password.");

    public static final ConfigOption<String> DRIVER =
            ConfigOptions.key("driver")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The class name of the JDBC driver to use to connect to this URL. "
                                    + "If not set, it will automatically be derived from the URL.");

    public static final ConfigOption<Duration> MAX_RETRY_TIMEOUT =
            ConfigOptions.key("connection.max-retry-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription("Maximum timeout between retries.");

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "The flush max size (includes all append, upsert and delete records), over this number"
                                    + " of records, will flush data.");

    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "The flush interval mills, over this time, asynchronous threads will flush data.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The max retry times if writing records to database failed.");
}
