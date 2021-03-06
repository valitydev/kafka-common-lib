package dev.vality.kafka.common.exception.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.springframework.util.backoff.FixedBackOff.UNLIMITED_ATTEMPTS;

/**
 * @deprecated in favor of {@link CommonErrorHandler} or {@link DefaultErrorHandler}
    or {@link ExponentialBackOffDefaultErrorHandler}.
 */
@Slf4j
@Deprecated
public class SeekToCurrentWithSleepErrorHandler extends SeekToCurrentErrorHandler {

    private final Integer sleepTimeSeconds;

    public SeekToCurrentWithSleepErrorHandler() {
        super.setAckAfterHandle(false);
        this.sleepTimeSeconds = 5;
    }

    public SeekToCurrentWithSleepErrorHandler(int sleepTimeSeconds, int maxFailures) {
        super(new FixedBackOff(0, maxFailures == -1 ? UNLIMITED_ATTEMPTS : (long) maxFailures - 1));
        super.setAckAfterHandle(false);
        this.sleepTimeSeconds = sleepTimeSeconds;
    }

    @Override
    public void handle(
            Exception thrownException,
            List<ConsumerRecord<?, ?>> records,
            Consumer<?, ?> consumer,
            MessageListenerContainer container) {
        log.error("Records commit failed", thrownException);
        this.sleepBeforeRetry();
        super.handle(thrownException, records, consumer, container);
    }

    private void sleepBeforeRetry() {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis((long) this.sleepTimeSeconds));
        } catch (InterruptedException var2) {
            Thread.currentThread().interrupt();
        }
    }
}
