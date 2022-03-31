package dev.vality.kafka.common.exception.handler;

import dev.vality.kafka.common.util.LogUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.RetryingBatchErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.List;

import static dev.vality.kafka.common.util.LogUtil.toSummaryString;

/**
 * An error handler that, for record listeners, seeks to the current offset for each topic
 * in the remaining records. Used to rewind partitions after a message failure so that it
 * can be replayed. For batch listeners, seeks to the current offset for each topic in a
 * batch of records. Used to rewind partitions after a message failure so that the batch
 * can be replayed. If the listener throws a {@link BatchListenerFailedException}, with
 * the failed record. The records before the record will have their offsets committed and
 * the partitions for the remaining records will be repositioned and/or the failed record
 * can be recovered and skipped. If some other exception is thrown, or a valid record is
 * not provided in the exception, error handling is delegated to a
 * {@link RetryingBatchErrorHandler} with this handler's {@link ExponentialBackOff}. If the record is
 * recovered, its offset is committed. This is a replacement for the legacy
 * {@link SeekToCurrentWithSleepErrorHandler} and {@link SeekToCurrentWithSleepBatchErrorHandler}
 */
@Slf4j
public class ExponentialBackOffDefaultErrorHandler extends DefaultErrorHandler {

    /**
     * Implementation of {@link BackOff} that increases the back off period for each
     * retry attempt. When the interval has reached the {@link ExponentialBackOff#setMaxInterval(long)
     * max interval}, it is no longer increased. Stops retrying once the
     * {@link ExponentialBackOff#setMaxElapsedTime(long) max elapsed time} has been reached.
     */
    public ExponentialBackOffDefaultErrorHandler() {
        super(new ExponentialBackOff());
    }

    /**
     * @param initialInterval the initial interval in milliseconds
     * @param multiplier      the multiplier (should be greater than or equal to 1)
     */
    public ExponentialBackOffDefaultErrorHandler(long initialInterval, double multiplier) {
        super(new ExponentialBackOff(initialInterval, multiplier));
    }

    /**
     * Subclass of {@link ExponentialBackOff} that allows the specification of the maximum
     * number of retries rather than the maximum elapsed time.
     * Construct an instance that will calculate the {@link ExponentialBackOff#setMaxElapsedTime(long)} from
     * the maxRetries.
     *
     * @param maxRetries the max retries.
     */
    public ExponentialBackOffDefaultErrorHandler(int maxRetries) {
        super(new ExponentialBackOffWithMaxRetries(maxRetries));
    }

    /**
     * False prevents the container from committing the offset of a recovered
     * record (when the error handler does not itself throw an exception).
     */
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
