package dev.vality.kafka.common.serialization;

import dev.vality.kafka.common.exception.KafkaSerializationException;
import dev.vality.kafka.common.exception.TransportException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.transport.TTransportException;

import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractThriftDeserializer<T extends TBase> implements Deserializer<T> {

    protected final ThreadLocal<TDeserializer> thriftDeserializer =
            ThreadLocal.withInitial(this::createNewDeserializer);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, ?> filtered = filterSslProperties(configs);
        log.warn("AbstractThriftDeserializer configure configs: {} isKey: {} is do nothing!", filtered, isKey);
    }

    public static Map<String, ?> filterSslProperties(Map<String, ?> configs) {
        return configs.entrySet().stream()
                .filter(entry -> !entry.getKey().contains("ssl"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public void close() {
        thriftDeserializer.remove();
    }

    protected T deserialize(byte[] data, T t) {
        try {
            thriftDeserializer.get().deserialize(t, data);
        } catch (Exception e) {
            log.error("Error when deserialize data", e);
            throw new KafkaSerializationException(e);
        }
        return t;
    }

    private TDeserializer createNewDeserializer() {
        try {
            return new TDeserializer();
        } catch (TTransportException ex) {
            throw new TransportException(ex);
        }
    }
}
