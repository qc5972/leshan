/*******************************************************************************
 * Copyright (c) 2020 Sierra Wireless and others.
 * 
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 * 
 * The Eclipse Public License is available at
 *    http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *    http://www.eclipse.org/org/documents/edl-v10.html.
 * 
 * Contributors:
 *     Sierra Wireless - initial API and implementation
 *******************************************************************************/
package org.eclipse.leshan.server.redis;

import java.util.Arrays;
import java.util.Random;

import org.eclipse.leshan.core.util.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.params.SetParams;

/**
 * An implementation of Redis Lock for the Jedis library usable in a single instance environment.
 * <p>
 * If you need to lock in an environment with N Redis masters, you should implement the
 * <a href="https://redis.io/topics/distlock#the-redlock-algorithm">RedLock algorithm</a>. If you succeed share the code
 * with us :-)
 * 
 * @see <a href="http://redis.io/topics/distlock#correct-implementation-with-a-single-instance"> algorithm details</a>
 * @since 1.1
 */
public class SingleInstanceJedisLock implements JedisLock {
    private static final Logger LOG = LoggerFactory.getLogger(SingleInstanceJedisLock.class);

    private static final Random RND = new Random();
    private final int expiration; // in ms
    private final long maxTime; // in ms
    private final long iterationTime; // in ms

    /**
     * Create a {@link SingleInstanceJedisLock} with {@code expiration} of 500ms, {@code maxTime} of 5000L and
     * {@code iterationTime} of 10ms
     * 
     * @see #SingleInstanceJedisLock(int, long, long)
     */
    public SingleInstanceJedisLock() {
        this(500, 5000L, 10L);
    }

    /**
     * @param expiration The lockKey expiration time in milliseconds. After this time the lock will be release even if
     *        {@link #release(Jedis, byte[], byte[])} is not called.
     * @param maxTime The maximum time to wait in milliseconds to acquire the lock. After this time an
     *        {@link IllegalStateException} is raised.
     * @param iterationTime The time to wait/sleep in milliseconds before each iteration when we try to acquire the
     *        lock.
     */
    public SingleInstanceJedisLock(int expiration, long maxTime, long iterationTime) {
        this.expiration = expiration;
        this.maxTime = maxTime;
        this.iterationTime = iterationTime;
    }

    /**
     * Try to acquires a lock for the given key. if it failed after {@code maxTime} raise an
     * {@link IllegalStateException}
     * 
     * @param j a Redis connection
     * @param lockKey the key to use as lock
     * @return a lock value that must be used to release the lock.
     */
    @Override
    public byte[] acquire(Jedis j, byte[] lockKey) throws IllegalStateException {
        long start = System.currentTimeMillis();

        byte[] randomLockValue = new byte[10];
        RND.nextBytes(randomLockValue);

        while (!"OK".equals(j.set(lockKey, randomLockValue, SetParams.setParams().nx().px(expiration)))) {
            if (System.currentTimeMillis() - start > maxTime)
                throw new IllegalStateException(
                        String.format("Could not acquire a lock from redis after waiting for %dms", maxTime));
            try {
                Thread.sleep(iterationTime);
            } catch (InterruptedException e) {
            }
        }
        return randomLockValue;
    }

    /**
     * Releases a lock for a given key and value.
     * 
     * @param j a Redis connection
     * @param lockKey the locked key
     * @param lockValue the value returned when the lock was acquired
     */
    @Override
    public void release(Jedis j, byte[] lockKey, byte[] lockValue) {
        if (lockValue != null) {
            // Watch the key to remove.
            j.watch(lockKey);

            byte[] prevousLockValue = j.get(lockKey);
            // Delete the key if needed.
            if (Arrays.equals(prevousLockValue, lockValue)) {
                // Try to delete the key
                Transaction transaction = j.multi();
                transaction.del(lockKey);
                boolean succeed = transaction.exec() != null;
                if (!succeed) {
                    LOG.warn(
                            "Failed to release lock for key {}/{}, meaning the key probably expired because of acquiring the lock for too long (more than {}ms)",
                            new String(lockKey), Hex.encodeHexString(lockValue), expiration);
                }
            } else {
                // the key must not be deleted.
                LOG.warn(
                        "Nothing to release for key {}/{}, meaning the key probably expired because of acquiring the lock for too long (more than {}ms)",
                        new String(lockKey), Hex.encodeHexString(lockValue), expiration);
                j.unwatch();
            }
        } else {
            LOG.warn("Trying to release a lock for {} with a null value", new String(lockKey));
        }
    }
}
