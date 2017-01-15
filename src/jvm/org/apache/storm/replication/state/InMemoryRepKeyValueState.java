package org.apache.storm.replication.state;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryRepKeyValueState<K, V> implements RepKeyValueState<K, V> {
	private static final Logger LOG = LoggerFactory.getLogger(InMemoryRepKeyValueState.class);
	private static final long DEFAULT_TXID = -1;
	private MyTxIdState<K, V> _savedState;
	private Map<K, V> _state = new ConcurrentHashMap<>();

	private static class MyTxIdState<K, V> {
		private long txid;
		private Map<K, V> state;

		public long getTxid()
		{
			return txid;
		}
		
		MyTxIdState(long txid, Map<K, V> state) {
			this.txid = txid;
			this.state = state;
		}

		@Override
		public String toString() {
			return "MyTxIdState{" + "txid=" + txid + ", state=" + state + '}';
		}
	}

	@Override
	public void save(long txid) {
		LOG.info("save state, txid {}", txid);
		_savedState = new MyTxIdState<>(txid, new ConcurrentHashMap<K, V>(_state));
	}

	@Override
	public void put(K key, V value) {
		_state.put(key, value);
	}

	@Override
	public V get(K key) {
		return _state.get(key);
	}

	@Override
	public V get(K key, V defaultValue) {
		V val = get(key);
		return val != null ? val : defaultValue;
	}

	@Override
	public String toString() {
		return "InMemoryKeyValueState{" + ", state=" + _state + '}';
	}

	public String varify() {
		if (_savedState == null)
			return "The state in memory is not initialized.";
		else
			return "The saved state " + _savedState;
	}

	@Override
	public void load(long txid) {
		LOG.info("Nothing to load, everything is in-memory.");
	}
}
