package org.apache.storm.replication.state;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.task.TopologyContext;

public class InMemoryRepKeyValueStateProvider implements RepStateProvider {

	private final ConcurrentHashMap<String, RepState> states = new ConcurrentHashMap<>();

	@Override
	public RepState newState(String namespace, Map stormConf, TopologyContext context) {
		RepState state = states.get(namespace);
		if (state == null) {
			RepState newState = new InMemoryRepKeyValueState<>();
			state = states.putIfAbsent(namespace, newState);
			if (state == null)
				state = newState;
		}

		return state;
	}

}
