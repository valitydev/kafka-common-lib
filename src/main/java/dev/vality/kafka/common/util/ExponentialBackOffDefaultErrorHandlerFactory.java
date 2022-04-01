package dev.vality.kafka.common.util;

import dev.vality.kafka.common.exception.handler.ExponentialBackOffDefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.ExponentialBackOff;

public class ExponentialBackOffDefaultErrorHandlerFactory {

    /**
     * Создается инстанс хендлера с дефолтными настройками ExponentialBackOff
     *
     * @see ExponentialBackOff#DEFAULT_INITIAL_INTERVAL
     * @see ExponentialBackOff#DEFAULT_MULTIPLIER
     * @see ExponentialBackOff#DEFAULT_MAX_INTERVAL
     * @see ExponentialBackOff#DEFAULT_MAX_ELAPSED_TIME
     */
    public static ExponentialBackOffDefaultErrorHandler create() {
        return new ExponentialBackOffDefaultErrorHandler(new ExponentialBackOff());
    }

    public static ExponentialBackOffDefaultErrorHandler create(
            long initialInterval,
            double multiplier,
            long maxInterval) {
        var exponentialBackOff = new ExponentialBackOff(initialInterval, multiplier);
        exponentialBackOff.setMaxInterval(maxInterval);
        return new ExponentialBackOffDefaultErrorHandler(exponentialBackOff);
    }

    public static ExponentialBackOffDefaultErrorHandler create(
            long initialInterval,
            double multiplier,
            long maxInterval,
            int maxRetries) {
        var exponentialBackOff = new ExponentialBackOffWithMaxRetries(maxRetries);
        exponentialBackOff.setInitialInterval(initialInterval);
        exponentialBackOff.setMultiplier(multiplier);
        exponentialBackOff.setMaxInterval(maxInterval);
        return new ExponentialBackOffDefaultErrorHandler(exponentialBackOff);
    }
}
