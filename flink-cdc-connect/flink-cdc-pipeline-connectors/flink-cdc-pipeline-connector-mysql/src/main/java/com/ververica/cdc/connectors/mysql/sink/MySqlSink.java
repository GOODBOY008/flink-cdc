package com.ververica.cdc.connectors.mysql.sink;

import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.common.sink.EventSinkProvider;
import com.ververica.cdc.common.sink.MetadataApplier;

/**
 * {@code DataSink} is used to write change data to external system and apply metadata changes to
 * external systems as well.
 */
public class MySqlSink implements DataSink {
  @Override
  public EventSinkProvider getEventSinkProvider() {
    return null;
  }

  @Override
  public MetadataApplier getMetadataApplier() {
    return null;
  }
}
