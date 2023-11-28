package com.ververica.cdc.connectors.mysql.source;

import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.common.source.EventSourceProvider;
import com.ververica.cdc.common.source.MetadataAccessor;

/**
 * {@code DataSource} is used to read change data from external system and fetch metadata from
 * external systems as well.
 */
public class MySqlSource implements DataSource {
    @Override
    public EventSourceProvider getEventSourceProvider() {
        return null;
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return null;
    }
}
