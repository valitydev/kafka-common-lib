package dev.vality.kafka.common.exception.handler;

import dev.vality.kafka.common.util.ExponentialBackOffDefaultErrorHandlerFactory;
import dev.vality.kafka.common.util.LogUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.List;

import static dev.vality.kafka.common.util.LogUtil.toSummaryString;

/**
 * Сейчас при создании {@link ConcurrentKafkaListenerContainerFactory} в качестве обработчика ошибок мы используем
 * реализации интерфейса {@link BatchErrorHandler} — {@link SeekToCurrentErrorHandler}
 * и {@link SeekToCurrentBatchErrorHandler}. В данной библиотеке эти хендлеры наследуют
 * {@link SeekToCurrentWithSleepBatchErrorHandler} и {@link SeekToCurrentWithSleepErrorHandler}. В данный момент эти
 * реализации являются устаревшими, а в spring boot 3 эти классы становятся package private, поэтому мы должны
 * избавиться от их использования.
 * <p>Вместо них официальная документация spring предлагает использовать {@link CommonErrorHandler} и его реализацию
 * {@link DefaultErrorHandler}.
 * <p>Данная библиотека предлагает класс {@link ExponentialBackOffDefaultErrorHandler}, который наследует
 * {@link DefaultErrorHandler}.
 * <p>Данный класс ограничивает {@link DefaultErrorHandler} использованием только экспоненциально замедляющегося
 * таймаута для перечитывания оффсета, что в свою очередь поможет не взрывать эластик. Также добавляется отключение
 * коммита оффсета после обработки хендлером {@link #isAckAfterHandle()}, а также дополнительное логгирование на
 * уровне этого хендлера средствами утилит этой библиотеки.
 * <p>Используйте {@link ExponentialBackOffDefaultErrorHandlerFactory} для создания инстанса этого класса
 */
@Slf4j
public class ExponentialBackOffDefaultErrorHandler extends DefaultErrorHandler {

    public ExponentialBackOffDefaultErrorHandler(ExponentialBackOff exponentialBackOff) {
        super(exponentialBackOff);
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
