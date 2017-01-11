package org.apache.storm.replication.state;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepStateFactory {

	private static final Logger LOG = LoggerFactory.getLogger(RepStateFactory.class);

	private static String DEFAULT_PROVIDER = "org.apache.storm.replication.state.InMemoryRepKeyValueStateProvider";

	public static RepState getState(String namespace, Map stormConf, TopologyContext context) {
		RepState state;
		try {
			String provider = null;
			if (stormConf.containsKey(Config.REDIS_PROVIDER)) {
				provider = "org.apache.storm.replication.state.RedisRepKeyValueStateProvider";
			} else {
				provider = DEFAULT_PROVIDER;
			}
			Class<?> klazz = Class.forName(provider);
			Object object = klazz.newInstance();
			if (object instanceof RepStateProvider) {
				state = ((RepStateProvider) object).newState(namespace, stormConf, context);
			} else {
				String msg = "Invalid state provider '" + provider
						+ "'. Should implement org.apache.storm.replication.state.RepStateProvider";
				LOG.error(msg);
				throw new RuntimeException(msg);
			}
		} catch (Exception ex) {
			LOG.error("Got exception while loading the state provider", ex);
			throw new RuntimeException(ex);
		}
		return state;
	}

}
