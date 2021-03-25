package com.microdb.transaction;

/**
 * TODO
 *
 * @author zhangjw
 * @version 1.0
 */
public class Test {

    void execute(TransactionID transactionID) {
        // ...
    }

    public static void main(String[] args) {

        Transaction transaction = new Transaction();
        TransactionID transactionId = transaction.getTransactionId();

        Test test = new Test();
        transaction.start();
        test.execute(transactionId);
        transaction.commit();

    }
}
