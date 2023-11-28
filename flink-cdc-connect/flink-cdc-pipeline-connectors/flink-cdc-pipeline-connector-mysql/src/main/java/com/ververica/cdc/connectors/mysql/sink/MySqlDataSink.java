package com.ververica.cdc.connectors.mysql.sink;

import com.ververica.cdc.common.factories.Factory;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.common.sink.EventSinkProvider;
import com.ververica.cdc.common.sink.FlinkSinkProvider;
import com.ververica.cdc.connectors.mysql.MySqlDatabase;

/**
 * {@code DataSink} is used to write change data to external system and apply metadata changes to
 * external systems as well.
 */
public class MySqlDataSink implements DataSink {
    private final Factory.Context context;

    public MySqlDataSink(Factory.Context context) {
        this.context = context;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkProvider.of(new MySqlsink());
    }

    @Override
    public MysqlMetadataApplier getMetadataApplier() {
        return new MysqlMetadataApplier((MysqlOptions) context);
    }
}
