package org.apache.storm.replication.state;

import java.util.Collections;
import java.util.Map;

import org.apache.storm.replication.redis.common.config.JedisPoolConfig;
import org.apache.storm.state.DefaultStateSerializer;
import org.apache.storm.state.Serializer;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RedisRepKeyValueStateProvider implements RepStateProvider {
	private static final Logger LOG = LoggerFactory.getLogger(RedisRepKeyValueStateProvider.class);

	@Override
	public RepState newState(String namespace, Map stormConf, TopologyContext context) {
		try {
			return getRedisKeyValueState(namespace, getStateConfig(stormConf));
		} catch (Exception ex) {
			LOG.error("Error loading config from storm conf {}", stormConf);
			throw new RuntimeException(ex);
		}
	}

	private RepState getRedisKeyValueState(String namespace, StateConfig config) throws Exception {
		return new RedisKeyValueRepState(namespace, getJedisPoolConfig(config), getKeySerializer(config),
				getValueSerializer(config));
	}

	private StateConfig getStateConfig(Map stormConf) throws Exception {
		StateConfig stateConfig = null;
		String providerConfig = null;
		ObjectMapper mapper = new ObjectMapper();
		mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
		if (stormConf.containsKey(org.apache.storm.Config.TOPOLOGY_STATE_PROVIDER_CONFIG)) {
			providerConfig = (String) stormConf.get(org.apache.storm.Config.TOPOLOGY_STATE_PROVIDER_CONFIG);
			stateConfig = mapper.readValue(providerConfig, StateConfig.class);
		} else {
			stateConfig = new StateConfig();
		}
		return stateConfig;
	}

	private Serializer getKeySerializer(StateConfig config) throws Exception {
		Serializer serializer = null;
		if (config.keySerializerClass != null) {
			Class<?> klass = (Class<?>) Class.forName(config.keySerializerClass);
			serializer = (Serializer) klass.newInstance();
		} else if (config.keyClass != null) {
			serializer = new DefaultStateSerializer(Collections.singletonList(Class.forName(config.keyClass)));
		} else {
			serializer = new DefaultStateSerializer();
		}
		return serializer;
	}

	private Serializer getValueSerializer(StateConfig config) throws Exception {
		Serializer serializer = null;
		if (config.valueSerializerClass != null) {
			Class<?> klass = (Class<?>) Class.forName(config.valueSerializerClass);
			serializer = (Serializer) klass.newInstance();
		} else if (config.valueClass != null) {
			serializer = new DefaultStateSerializer(Collections.singletonList(Class.forName(config.valueClass)));
		} else {
			serializer = new DefaultStateSerializer();
		}
		return serializer;
	}

	private JedisPoolConfig getJedisPoolConfig(StateConfig config) {
		return config.jedisPoolConfig != null ? config.jedisPoolConfig : new JedisPoolConfig.Builder().build();
	}

	static class StateConfig {
		String keyClass;
		String valueClass;
		String keySerializerClass;
		String valueSerializerClass;
		JedisPoolConfig jedisPoolConfig;

		@Override
		public String toString() {
			return "StateConfig{" + "keyClass='" + keyClass + '\'' + ", valueClass='" + valueClass + '\''
					+ ", keySerializerClass='" + keySerializerClass + '\'' + ", valueSerializerClass='"
					+ valueSerializerClass + '\'' + ", jedisPoolConfig=" + jedisPoolConfig + '}';
		}
	}

}
