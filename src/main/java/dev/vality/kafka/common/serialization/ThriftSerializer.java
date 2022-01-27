package dev.vality.kafka.common.serialization;

import dev.vality.kafka.common.exception.KafkaSerializationException;
import dev.vality.kafka.common.exception.TransportException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.transport.TTransportException;

import java.util.Map;

import static dev.vality.kafka.common.serialization.AbstractThriftDeserializer.filterSslProperties;

@Slf4j
public class ThriftSerializer<T extends TBase> implements Serializer<T> {

    private final ThreadLocal<TSerializer> thriftSerializer = ThreadLocal.withInitial(this::createNewSeriaflizer);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, ?> filtered = filterSslProperties(configs);
        log.warn("ThriftSerializer configure configs: {} isKey: {} is do nothing!", filtered, isKey);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        log.debug("Serialize message, topic: {}, data: {}", topic, data);
        try {
            return thriftSerializer.get().serialize(data);
        } catch (TException e) {
            log.error("Error when serialize data: {} ", data, e);
            throw new KafkaSerializationException(e);
        }
    }

    @Override
    public void close() {
        thriftSerializer.remove();
    }

    private TSerializer createNewSeriaflizer() {
        try {
            return new TSerializer();
        } catch (TTransportException ex) {
            throw new TransportException(ex);
        }
    }
}
