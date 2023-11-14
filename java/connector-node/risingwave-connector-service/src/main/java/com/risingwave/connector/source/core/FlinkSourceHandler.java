package com.risingwave.connector.source.core;

import com.risingwave.connector.*;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;

import static com.risingwave.java.utils.ObjectSerde.serializeObject;

public class FlinkSourceHandler {
    private FlinkSource flinkSource;

    private final FlinkDynamicSourceHandleConfig flinkDynamicSourceHandleConfig;

    SourceContextImpl sourceContext;

    public FlinkSourceHandler(FlinkSource flinkSource,FlinkDynamicSourceHandleConfig flinkDynamicSourceHandleConfig,long channelPtr){
        var jsonConverter = new DbzJsonConverter();
        final HashMap<String, Object> configs = new HashMap<>(2);
        configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
        jsonConverter.configure(configs);

        this.flinkSource = flinkSource;
        this.flinkDynamicSourceHandleConfig = flinkDynamicSourceHandleConfig;
        serializeObject(flinkDynamicSourceHandleConfig.get)
        sourceContext = new SourceContextImpl(channelPtr,flinkDynamicSourceHandleConfig.getColumns(),s,jsonConverter);
    }
    public void startFlinkDynamicSource(long channelPtr,long checkpointId) {

        flinkSource.init(new FunctionInitializationContextImpl(checkpointId,operatorStateStore));
        flinkSource.open();

    }

    public void getState(long checkpoint,long channelPtr) {
        sourceContext.barrier();
        flinkSource.saveStates(checkpoint);
        flinkSource.notifyCheckpointComplete(checkpoint);
    }
}
