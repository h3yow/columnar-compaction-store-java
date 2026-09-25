package com.github.h3yow.columnarcompactionstorejava.store;

/** Exception carrying a machine-distinguishable {@link ErrorCode}. */
public class StoreException extends RuntimeException {
    private final ErrorCode code;

    public StoreException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public StoreException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ErrorCode code() {
        return code;
    }
}
