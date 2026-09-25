package com.github.h3yow.columnarcompactionstorejava.store.exception;

/**
 * 存储层非批次校验类错误（IO、资源、损坏等），携带可区分原因。
 */
public class StoreException extends RuntimeException {

    public enum Reason {
        IO_ERROR,
        BAD_FORMAT,
        CRC_MISMATCH,
        RESOURCE_TIMEOUT,
        MEMORY_LIMIT_EXCEEDED,
        STORE_CLOSED,
        INTERRUPTED
    }

    private final Reason reason;

    public StoreException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public StoreException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}
