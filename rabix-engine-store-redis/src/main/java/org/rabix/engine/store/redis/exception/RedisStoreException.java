package org.rabix.engine.store.redis.exception;

public class RedisStoreException extends RuntimeException {

    public RedisStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public RedisStoreException(Throwable cause) {
        super(cause);
    }
}
