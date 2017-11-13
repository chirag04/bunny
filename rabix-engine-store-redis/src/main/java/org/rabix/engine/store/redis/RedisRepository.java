package org.rabix.engine.store.redis;

import java.util.List;

public interface RedisRepository {

    void set(String namespace, String key, Object value);

    <T> T get(String namespace, String key, Class<T> type);

    <T> List<T> getAll(String namespace, Class<T> type);

    void delete(String namespace, String key);

    void deleteAll(String namespace);

    void deleteAll(String namespace, String key);

    void flushAll();

    void append(String namespace, String key, Object value);

    <T> List<T> getList(String namespace, String key, Class<T> type);

    void setInList(String namespace, String key, int index, Object value);

    void removeFromList(String namespace, String key, Object value);
}
