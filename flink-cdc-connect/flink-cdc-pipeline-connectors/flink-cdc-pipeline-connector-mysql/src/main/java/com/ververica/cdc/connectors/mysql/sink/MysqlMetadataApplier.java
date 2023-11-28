package com.ververica.cdc.connectors.mysql.sink;

import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.sink.MetadataApplier;
import com.ververica.cdc.common.types.DataType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** {@code MetadataApplier} is used to apply metadata changes to MySQL. */
public class MysqlMetadataApplier implements MetadataApplier {

    private final MysqlOptions options;

    public MysqlMetadataApplier(MysqlOptions options) {
        this.options = options;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        String ddlSql = getDDLBySchemaChangeEvent(schemaChangeEvent);
        // execute ddl sql
    }

    @SuppressWarnings("unchecked")
    public String getDDLBySchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) {

        StringBuilder ddlSql = new StringBuilder();
        if (schemaChangeEvent.getClass().equals(AddColumnEvent.class)) {
            List<AddColumnEvent.ColumnWithPosition> addedColumns =
                    ((AddColumnEvent) schemaChangeEvent).getAddedColumns();
            ddlSql.append("ALTER TABLE ");
            ddlSql.append(schemaChangeEvent.tableId().toString());
            for (int i = 0; i < addedColumns.size(); i++) {
                ddlSql.append(" ADD COLUMN ");
                ddlSql.append(addedColumns.get(i).getAddColumn().getName());
                ddlSql.append(" ");
                ddlSql.append(addedColumns.get(i).getAddColumn().getType().toString());
                if (i != addedColumns.size() - 1) {
                    ddlSql.append(",");
                }
            }
            return ddlSql.append(";").toString();
        } else if (schemaChangeEvent.getClass().equals(DropColumnEvent.class)) {
            List<Column> droppedColumns = ((DropColumnEvent) schemaChangeEvent).getDroppedColumns();
            ddlSql.append("ALTER TABLE ");
            ddlSql.append(schemaChangeEvent.tableId().toString());
            for (int i = 0; i < droppedColumns.size(); i++) {
                ddlSql.append(" DROP COLUMN ");
                ddlSql.append(droppedColumns.get(i).getName());
                if (i != droppedColumns.size() - 1) {
                    ddlSql.append(",");
                }
            }
            return ddlSql.append(";").toString();
        } else if (schemaChangeEvent.getClass().equals(RenameColumnEvent.class)) {
            Map<String, String> nameMapping =
                    ((RenameColumnEvent) schemaChangeEvent).getNameMapping();
            ddlSql.append("ALTER TABLE ");
            ddlSql.append(schemaChangeEvent.tableId().toString());
            Object[] nameMappingArr = nameMapping.entrySet().toArray();
            for (int i = 0; i < nameMappingArr.length; i++) {
                ddlSql.append(" RENAME COLUMN ");
                ddlSql.append(((Map.Entry<String, String>) nameMappingArr[i]).getKey());
                ddlSql.append(" TO ");
                ddlSql.append(((Map.Entry<String, String>) nameMappingArr[i]).getValue());
                if (i != nameMappingArr.length - 1) {
                    ddlSql.append(",");
                }
            }
            return ddlSql.append(";").toString();
        } else if (schemaChangeEvent.getClass().equals(AlterColumnTypeEvent.class)) {
            Map<String, DataType> typeMapping =
                    ((AlterColumnTypeEvent) schemaChangeEvent).getTypeMapping();
            ddlSql.append("ALTER TABLE ");
            ddlSql.append(schemaChangeEvent.tableId().toString());
            Object[] typeMappingArr = typeMapping.entrySet().toArray();
            for (int i = 0; i < typeMappingArr.length; i++) {
                ddlSql.append(" MODIFY COLUMN ");
                ddlSql.append(((Map.Entry<String, DataType>) typeMappingArr[i]).getKey());
                ddlSql.append(" ");
                // todo typeMapping.getValue().asSummaryString() is not correct
                ddlSql.append(
                        ((Map.Entry<String, DataType>) typeMappingArr[i])
                                .getValue()
                                .asSummaryString());
                if (i != typeMappingArr.length - 1) {
                    ddlSql.append(",");
                }
            }
            return ddlSql.append(";").toString();
        } else if (schemaChangeEvent.getClass().equals(CreateTableEvent.class)) {
            Schema schema = ((CreateTableEvent) schemaChangeEvent).getSchema();
            ddlSql.append("CREATE TABLE ");
            ddlSql.append(schemaChangeEvent.tableId().toString());
            ddlSql.append(" (");
            List<Column> columnList = schema.getColumns();
            for (int i = 0; i < columnList.size(); i++) {
                ddlSql.append(columnList.get(i).getName());
                ddlSql.append(" ");
                ddlSql.append(columnList.get(i).getType().asSummaryString());
                if (i != columnList.size() - 1) {
                    ddlSql.append(",");
                }
            }
            Optional.of(schema.primaryKeys())
                    .ifPresent(
                            primaryKeys -> {
                                ddlSql.append(", PRIMARY KEY (");
                                for (int i = 0; i < primaryKeys.size(); i++) {
                                    ddlSql.append(primaryKeys.get(i));
                                    if (i != primaryKeys.size() - 1) {
                                        ddlSql.append(",");
                                    }
                                }
                                ddlSql.append(")");
                            });
            ddlSql.append(");");
            return ddlSql.toString();
        } else {
            throw new RuntimeException("Unsupported SchemaChangeEvent: " + schemaChangeEvent);
        }
    }
}
