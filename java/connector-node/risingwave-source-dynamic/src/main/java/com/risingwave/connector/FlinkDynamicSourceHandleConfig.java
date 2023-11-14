package com.risingwave.connector;

import com.risingwave.connector.api.ColumnDesc;
import com.risingwave.connector.api.TableSchema;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.List;

public class FlinkDynamicSourceHandleConfig {

    public FlinkDynamicSourceHandleConfig(){
    }

    public String getConnector(){
        return "";
    }

    public TableSchema getColumns(){
        return null;
    }
}
