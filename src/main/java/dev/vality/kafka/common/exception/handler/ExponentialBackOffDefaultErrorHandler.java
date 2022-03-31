package dev.vality.kafka.common.exception.handler;

import dev.vality.kafka.common.util.LogUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.List;

import static dev.vality.kafka.common.util.LogUtil.toSummaryString;

@Slf4j
public class ExponentialBackOffDefaultErrorHandler extends DefaultErrorHandler {

    public ExponentialBackOffDefaultErrorHandler() {
        this(new ExponentialBackOff());
    }

    public ExponentialBackOffDefaultErrorHandler(BackOff backOff) {
        super(backOff);
    }

    @Override
    public boolean isAckAfterHandle() {
        return false;
    }

    @Override
    public void handleRemaining(
            Exception thrownException,
            List<ConsumerRecord<?, ?>> records,
            Consumer<?, ?> consumer,
            MessageListenerContainer container) {
        log.error(
                String.format("Records commit failed, size=%d, %s", records.size(), LogUtil.toString(records)),
                thrownException);
        super.handleRemaining(thrownException, records, consumer, container);
    }

    @Override
    public void handleBatch(
            Exception thrownException,
            ConsumerRecords<?, ?> data,
            Consumer<?, ?> consumer,
            MessageListenerContainer container,
            Runnable invokeListener) {
        log.error(
                String.format("Records commit failed, size=%d, %s", data.count(), toSummaryString(data)),
                thrownException);
        super.handleBatch(thrownException, data, consumer, container, invokeListener);
    }
}
