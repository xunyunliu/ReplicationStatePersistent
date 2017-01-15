package org.apache.storm.replication.state;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.replication.redis.common.config.JedisPoolConfig;
import org.apache.storm.replication.redis.common.container.JedisCommandsContainerBuilder;
import org.apache.storm.replication.redis.common.container.JedisCommandsInstanceContainer;
import org.apache.storm.state.DefaultStateSerializer;
import org.apache.storm.state.Serializer;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCommands;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

public class RedisKeyValueRepState<K, V> implements RepKeyValueState<K, V> {

	private static final Logger LOG = LoggerFactory.getLogger(RedisKeyValueRepState.class);
	private static final String TXID_KEY = "txid";
	private static final int MAX_WAIT_SECONDS = 30;

	// turn byte[] into String and vice versa
	private final BASE64Encoder base64Encoder;
	private final BASE64Decoder base64Decoder;

	// the namespace to store task states
	private final String namespace;

	// the namespace to store transaction id
	private final String txidNamespace;

	// serialize the in-memory map;
	private final Serializer<K> keySerializer;
	private final Serializer<V> valueSerializer;

	// the container that hosts the jedis client
	private final JedisCommandsInstanceContainer jedisContainer;

	// format: <K,V> K = "txid", V = "3868854776081394153"
	private Map<String, String> txIds;

	// user data
	private Map<String, String> inMemoryMap;

	public RedisKeyValueRepState(String namespace) {
		this(namespace, new JedisPoolConfig.Builder().build());
	}

	public RedisKeyValueRepState(String namespace, JedisPoolConfig poolConfig) {
		this(namespace, poolConfig, new DefaultStateSerializer<K>(), new DefaultStateSerializer<V>());
	}

	public RedisKeyValueRepState(String namespace, JedisPoolConfig poolConfig, Serializer<K> keySerializer,
			Serializer<V> valueSerializer) {
		this(namespace, JedisCommandsContainerBuilder.build(poolConfig), keySerializer, valueSerializer);
	}

	public RedisKeyValueRepState(String namespace, JedisCommandsInstanceContainer jedisContainer,
			Serializer<K> keySerializer, Serializer<V> valueSerializer) {
		base64Encoder = new BASE64Encoder();
		base64Decoder = new BASE64Decoder();
		this.namespace = namespace;
		this.txidNamespace = namespace + "$txid";
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.jedisContainer = jedisContainer;
		this.inMemoryMap = new ConcurrentHashMap<>();
		this.txIds = new HashMap<>();
	}

	public void load(long txid) {
		LOG.debug("Load in-memory state from redis, txid {}", txid);
		JedisCommands commands = null;
		try {
			commands = jedisContainer.getInstance();
			int attempts = 0;
			while (!checkStateExistence(commands, txid) && attempts < MAX_WAIT_SECONDS) {
				attempts++;
				Utils.sleep(1000);
			}
			if (attempts < MAX_WAIT_SECONDS) {
				txIds = commands.hgetAll(txidNamespace);
				inMemoryMap = new ConcurrentHashMap<>(Collections.unmodifiableMap(commands.hgetAll(namespace)));
			} else {
				LOG.error("Error! no saved state has been found in Redis, txid {}, thread {}", txid,
						Thread.currentThread().getName());
			}
		} finally {
			jedisContainer.returnInstance(commands);
		}
	}

	private boolean checkStateExistence(JedisCommands commands, long txid) {
		if (commands.exists(txidNamespace)) {
			Map<String, String> txIdsInRedis = commands.hgetAll(txidNamespace);
			if (txIdsInRedis.get(TXID_KEY).equals(String.valueOf(txid)))
				return true;
		}
		return false;
	}

	@Override
	public void save(long txid) {
		LOG.debug("Save in-memory state to redis, txid {}", txid);
		JedisCommands commands = null;
		try {
			commands = jedisContainer.getInstance();
			if (checkStateExistence(commands, txid)) {
				LOG.debug("The state has been persisted in Redis.");
				return;
			}
			if (!inMemoryMap.isEmpty()) {
				commands.hmset(namespace, inMemoryMap);
			} else {
				LOG.debug("Nothing to save, txid {}", txid);
			}
			txIds.put(TXID_KEY, String.valueOf(txid));
			commands.hmset(txidNamespace, txIds);
			LOG.info("{} has saved the state, txid {}", Thread.currentThread().getName(), txid);
		} finally {
			jedisContainer.returnInstance(commands);
		}
	}

	@Override
	public void put(K key, V value) {
		LOG.debug("put key '{}', value '{}'", key, value);
		inMemoryMap.put(encode(keySerializer.serialize(key)), encode(valueSerializer.serialize(value)));

	}

	@Override
	public V get(K key) {
		LOG.debug("get key '{}'", key);
		String redisKey = encode(keySerializer.serialize(key));
		String redisValue = null;
		if (inMemoryMap.containsKey(redisKey)) {
			redisValue = inMemoryMap.get(redisKey);
		}
		V value = null;
		if (redisValue != null) {
			value = valueSerializer.deserialize(decode(redisValue));
		}
		LOG.debug("Value for key '{}' is '{}'", key, value);
		return value;
	}

	@Override
	public V get(K key, V defaultValue) {
		V val = get(key);
		return val != null ? val : defaultValue;
	}

	private String encode(byte[] bytes) {
		return base64Encoder.encode(bytes);
	}

	private byte[] decode(String s) {
		try {
			return base64Decoder.decodeBuffer(s);
		} catch (IOException ex) {
			throw new RuntimeException("Error while decoding string " + s);
		}
	}

	@Override
	public String toString() {
		return "RedisKeyValueRepState [inMemoryMap=" + inMemoryMap + "]";
	}

}
