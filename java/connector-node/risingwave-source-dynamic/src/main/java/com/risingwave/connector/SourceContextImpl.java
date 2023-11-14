package com.risingwave.connector;

import com.mysql.cj.result.Row;
import com.risingwave.java.binding.Binding;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.TaskServiceOuterClass;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SourceContextImpl<T> implements SourceFunction.SourceContext<T> {
    long channelPtr;
    int sourceId;
    Object checkpointLock = new Object();

    JsonConverter converter;

    List<ConnectorServiceProto.CdcMessage> messageList;

    Schema schema;
    public SourceContextImpl(long channelPtr,int sourceId,Schema schema,JsonConverter converter,FlinkSource flinkSource){
        this.converter = converter;
        this.channelPtr = channelPtr;
        this.sourceId = sourceId;
        this.messageList = new ArrayList<>();
        this.schema = schema;
    }

    private void sendMessage() {
        ConnectorServiceProto.GetEventStreamResponse.Builder builder = ConnectorServiceProto.GetEventStreamResponse.newBuilder();
        builder.addAllEvents(messageList);
        builder.setSourceId(sourceId);
        ConnectorServiceProto.GetEventStreamResponse getEventStreamResponse = builder.build();
        if (!Binding.sendCdcSourceMsgToChannel(channelPtr, getEventStreamResponse.toByteArray())){
            throw new RuntimeException("Send response fail");
        }
        messageList = new ArrayList<>();
    }

    public void barrier(){
        synchronized (checkpointLock){
            sendMessage();
        }
    }

    @Override
    public void collect(T t) {
        if (t instanceof GenericRowData){
            GenericRowData t1 = (GenericRowData) t;
            ConnectorServiceProto.CdcMessage.Builder builder = ConnectorServiceProto.CdcMessage.newBuilder();
            builder.setPartition(String.valueOf(sourceId));

            Struct struct = new Struct(schema);
            for (int i = 0; i < t1.getArity(); i++) {
                Object field = t1.getField(i);
                struct.put(schema.name(),field);
            }

            byte[] payload = converter.fromConnectData("topic", struct.schema(), struct);
            builder.setPayload(new String(payload, StandardCharsets.UTF_8));
            ConnectorServiceProto.CdcMessage cdcMessage = builder.build();
            messageList.add(cdcMessage);
            if (messageList.size() > 512){
                sendMessage();
            }
        }else{
            throw  new NotSupportException();
        }
    }

    @Override
    public void collectWithTimestamp(T t, long l) {
        throw new NotSupportException();
    }

    @Override
    public void emitWatermark(Watermark watermark) {
        throw new NotSupportException();
    }

    @Override
    public void markAsTemporarilyIdle() {

    }

    @Override
    public Object getCheckpointLock() {
        return checkpointLock;
    }

    @Override
    public void close() {

    }
}
