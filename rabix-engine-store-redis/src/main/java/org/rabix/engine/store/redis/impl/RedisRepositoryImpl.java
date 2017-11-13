package org.rabix.engine.store.redis.impl;

import org.rabix.engine.store.redis.RedisRepository;
import org.rabix.engine.store.redis.exception.RedisStoreException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class RedisRepositoryImpl implements RedisRepository {

    private final JedisPool jedisPool;

    public RedisRepositoryImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public void set(String namespace, String key, Object value) {
        if (value == null) {
            return;
        }

        try(Jedis jedis = jedisPool.getResource()) {
            byte[] serialized = serialize(value);
            jedis.set(inNamespace(namespace, key), serialized);
        } catch (IOException e) {
            throw new RedisStoreException("Unable to serialize value for key " + key, e);
        }
    }

    @Override
    public <T> T get(String namespace, String key, Class<T> type) {
        try (Jedis jedis = jedisPool.getResource()) {
            byte[] serialized = jedis.get(inNamespace(namespace, key));
            return deserialize(serialized, type);
        } catch (IOException | ClassNotFoundException e) {
            throw new RedisStoreException("Unable to deserialize value for key " + key, e);
        }
    }

    @Override
    public <T> List<T> getAll(String namespace, Class<T> type) {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<byte[]> keys = keysInNamespace(namespace);

            if (keys.isEmpty()) {
                return Collections.emptyList();
            }

            List<byte[]> jsons = jedis.mget(keys.toArray(new byte[][]{}));
            return jsons
                    .stream()
                    .filter(Objects::nonNull)
                    .map(serialized -> {
                        try {
                            return deserialize(serialized, type);
                        } catch (IOException | ClassNotFoundException e) {
                            throw new RedisStoreException("Unable to deserialize value!", e);
                        }
                    })
                    .collect(Collectors.toList());
        }
    }

    @Override
    public void delete(String namespace, String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(inNamespace(namespace, key));
        }
    }

    @Override
    public void deleteAll(String namespace) {
        deleteAll(namespace, null);
    }

    @Override
    public void deleteAll(String namespace, String key) {
        Set<byte[]> keysToDelete = keysInNamespace(namespace + ":" + ((key == null) ? "" : key));

        try (Jedis jedis = jedisPool.getResource()) {
            keysToDelete.forEach(jedis::del);
        }
    }

    @Override
    public void flushAll() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.flushAll();
        }
    }

    @Override
    public void append(String namespace, String key, Object value) {
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.rpush(inNamespace(namespace, key), serialize(value));
        } catch (IOException e) {
            throw new RedisStoreException("Unable to rpush values for key " + key + " in namespace " + namespace, e);
        }
    }

    @Override
    public <T> List<T> getList(String namespace, String key, Class<T> type) {
        try(Jedis jedis = jedisPool.getResource()) {
            List<byte[]> list;
            if (key == null) {
                List<byte[]> keys = new ArrayList<>(keysInNamespace(namespace));
                if (keys.isEmpty()) {
                    return Collections.emptyList();
                }
                list = keys
                        .stream()
                        .map(listKey -> jedis.lrange(listKey, 0, Integer.MAX_VALUE))
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
            } else {
                list = jedis.lrange(inNamespace(namespace, key), 0, Integer.MAX_VALUE);
            }

            if (list == null) {
                return Collections.emptyList();
            }
            return deserialize(list, type);
        } catch (IOException | ClassNotFoundException e) {
            throw new RedisStoreException("Failed to getList for namespace: " + namespace + " and key: " + key, e);
        }
    }

    @Override
    public void setInList(String namespace, String key, int index, Object value) {
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.lset(inNamespace(namespace, key), index, serialize(value));
        } catch (IOException e) {
            throw new RedisStoreException("Could not set value on index " + index + "for namespace " + namespace + " and key " + key, e);
        }
    }

    @Override
    public void removeFromList(String namespace, String key, Object value) {
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.lrem(inNamespace(namespace, key), 1, serialize(value));
        } catch (IOException e) {
            throw new RedisStoreException("Could not remove value from list in namespace " + namespace + " with key " + key, e);
        }
    }

    private byte[] inNamespace(String namespace, String key) {
        return (namespace + ":" + (key == null ? "" : key)).getBytes();
    }

    private Set<byte[]> keysInNamespace(String namespace) {
        try(Jedis jedis = jedisPool.getResource()) {
            return jedis.keys(namespace + "*").stream().map(String::getBytes).collect(Collectors.toSet());
        }
    }

    private byte[] serialize(Object value) throws IOException {
        if (value == null) {
            return null;
        }

        try(ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream)) {
            objectOutputStream.writeObject(value);

            return byteOutputStream.toByteArray();
        }
    }

    private <T> T deserialize(byte[] serialized, Class<T> type) throws IOException, ClassNotFoundException {
        if (serialized == null) {
            return null;
        }

        try(ByteArrayInputStream byteInputStream = new ByteArrayInputStream(serialized);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteInputStream)) {
            Object object = objectInputStream.readObject();

            return type.cast(object);
        }
    }

    private <T> List<T> deserialize(List<byte[]> serialized, Class<T> type) throws IOException, ClassNotFoundException {
        List<T> deserialized = new ArrayList<>();
        for (byte[] s : serialized) {
            deserialized.add(deserialize(s, type));
        }
        return deserialized;
    }
}
