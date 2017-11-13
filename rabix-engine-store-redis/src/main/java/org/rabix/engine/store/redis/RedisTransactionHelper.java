package org.rabix.engine.store.redis;

import org.rabix.engine.store.repository.TransactionHelper;

public class RedisTransactionHelper extends TransactionHelper {

    @Override
    public <Result> Result doInTransaction(TransactionCallback<Result> callback) throws Exception {
        return callback.call();
    }
}
