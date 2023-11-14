package com.risingwave.connector;

import com.risingwave.connector.api.ColumnDesc;
import com.risingwave.connector.api.source.SourceHandler;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FlinkDynamicSourceFactory{

    public FlinkSource buildFlinkSource(FlinkDynamicSourceHandleConfig config){

        FactoryUtil.DefaultDynamicTableContext defaultDynamicTableContext = buildDefaultDynamicTableContext(config);

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        DynamicTableSourceFactory dynamicTableSinkFactory =
                FactoryUtil.discoverFactory(
                        contextClassLoader, DynamicTableSourceFactory.class, config.getConnector());

        ScanTableSource dynamicTableSource = (ScanTableSource) dynamicTableSinkFactory.createDynamicTableSource(defaultDynamicTableContext);
        ScanTableSource.ScanRuntimeProvider scanRuntimeProvider = dynamicTableSource.getScanRuntimeProvider(new ScanRuntimeProviderContext());
        if (scanRuntimeProvider instanceof SourceFunctionProvider) {
            final SourceFunctionProvider sourceFunctionProvider = (SourceFunctionProvider) scanRuntimeProvider;
            final SourceFunction<RowData> function = sourceFunctionProvider.createSourceFunction();
            return new SourceFunctionImpl(function);
        }
//        else if (scanRuntimeProvider instanceof InputFormatProvider) {
//            throw new UnsupportedOperationException(" is unsupported now.");
//        }
        else if (scanRuntimeProvider instanceof SourceProvider) {
            final Source<RowData, ?, ?> source = ((SourceProvider) scanRuntimeProvider).createSource();
            throw new NotSupportException();
        }
//        else if (scanRuntimeProvider instanceof DataStreamScanProvider) {
//            throw new UnsupportedOperationException(" is unsupported now.");
//        } else if (scanRuntimeProvider instanceof TransformationScanProvider) {
//            throw new UnsupportedOperationException(" is unsupported now.");
//        }
        else {
            throw new NotSupportException();
        }

    }

    public FactoryUtil.DefaultDynamicTableContext buildDefaultDynamicTableContext(FlinkDynamicSourceHandleConfig config){
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of("","","");
//        List<Column> flinkColumns = FlinkDynamicAdaptUtil.getFlinkColumnsFromSchema(tableSchema);
        List<Column> flinkColumns = new ArrayList<>();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        DynamicTableSourceFactory dynamicTableSourceFactory =
                FactoryUtil.discoverFactory(
                        contextClassLoader, DynamicTableSourceFactory.class, config.getConnector());
        Schema.Builder schemaBuilder = Schema.newBuilder();

        for (ColumnDesc columnDesc : config.getColumns().getColumnDescs()) {
            schemaBuilder.column(
                    columnDesc.getName(), columnDesc.getDataType().toString());
        }

        Schema shcema = schemaBuilder.primaryKey(config.getColumns().getPrimaryKeys()).build();
        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                shcema, null, config.getColumns().getPrimaryKeys(),
//                                config.getOption()
                                new HashMap<>()
                        ),
                        ResolvedSchema.of(flinkColumns));
        return new FactoryUtil.DefaultDynamicTableContext(
                objectIdentifier,
                resolvedCatalogTable,
                new HashMap<String, String>(),
                new Configuration(),
                contextClassLoader,
                false);
    }

}
