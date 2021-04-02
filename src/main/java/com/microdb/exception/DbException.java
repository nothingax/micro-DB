package com.microdb.exception;

/**
 * 异常封装
 *
 * @author zhangjw
 * @version 1.0
 */
public class DbException extends RuntimeException {
    private static final long serialVersionUID = -234831968729282743L;

    public DbException() {
    }

    public DbException(String message) {
        super(message);
    }

    public DbException(String message, Throwable cause) {
        super(message, cause);
    }

    public DbException(Throwable cause) {
        super(cause);
    }

    public DbException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
