package dev.vality.kafka.common.exception.handler;

import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler;

import java.util.concurrent.TimeUnit;

import static dev.vality.kafka.common.util.LogUtil.toSummaryString;

/**
 * @deprecated in favor of {@link CommonErrorHandler} or {@link DefaultErrorHandler}
 * or {@link ExponentialBackOffDefaultErrorHandler}.
 */
@Slf4j
@Deprecated
public class SeekToCurrentWithSleepBatchErrorHandler extends SeekToCurrentBatchErrorHandler {

    @Setter
    private boolean ackAfterHandle = false;

    private final Integer sleepTimeSeconds;

    public SeekToCurrentWithSleepBatchErrorHandler(Integer sleepTimeSeconds) {
        super();
        this.sleepTimeSeconds = sleepTimeSeconds;
    }

    public SeekToCurrentWithSleepBatchErrorHandler() {
        super();
        this.sleepTimeSeconds = 5;
    }

    @Override
    public void handle(
            Exception thrownException,
            ConsumerRecords<?, ?> data,
            Consumer<?, ?> consumer,
            MessageListenerContainer container) {
        log.error(String.format("Records commit failed, size=%d, %s", data.count(),
                toSummaryString((ConsumerRecords<String, MachineEvent>) data)
        ), thrownException);

        sleepBeforeRetry();

        super.handle(thrownException, data, consumer, container);
    }

    @Override
    public boolean isAckAfterHandle() {
        return this.ackAfterHandle;
    }

    private void sleepBeforeRetry() {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepTimeSeconds));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}