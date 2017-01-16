package org.apache.storm.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.storm.replication.state.RepState;
import org.apache.storm.replication.state.RepStateFactory;
import org.apache.storm.spout.ReplicationSpout;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.ReplicationUtils;
import org.apache.storm.utils.Utils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationStatefulBoltExecutor<T extends RepState> extends BaseReplicationBoltExecutor {

	private static final Logger LOG = LoggerFactory.getLogger(ReplicationStatefulBoltExecutor.class);
	private final IRepStatefulBolt<T> _bolt;
	private RepState _state;
	private SelectiveEmitOutputCollector _collector;

	// number of replications set for this bolt
	private int _numReplications;
	// the real state id (note that replication tasks are sharing the same
	// states)
	private int _realStateID;
	// the namespace used to store real state
	private String _namespace;
	// if this task has been initialized
	private boolean _isInitialized = false;
	// is this the primary task that is allowed to emit output to down streams.
	private boolean _isPrimary = false;
	// temporarily hold the incoming tuples when the task has not been
	// initialized.
	private List<Tuple> _pendingTuples = new ArrayList<>();

	private static CuratorFramework _client;
	private static String _connectString;
	private static int _retryCount;
	private static int _retryInterval;

	public ReplicationStatefulBoltExecutor(IRepStatefulBolt<T> bolt) {
		_bolt = bolt;
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		init(context, collector);
		prepareMememberVariables(stormConf, context);
		this._collector = new SelectiveEmitOutputCollector(collector, _isPrimary);
		_bolt.prepare(stormConf, context, this._collector);
		_bolt.initState((T) _state);
	}

	private void prepareMememberVariables(Map stormConf, TopologyContext context) {
		prepareConectionConfig(stormConf);
		
		this._numReplications = ReplicationUtils.loadNumReplications(context);
		LOG.info("The global number of replications is {}.", _numReplications);
		int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
		if (numTasks % _numReplications != 0) {
			throw new IllegalArgumentException("The number of tasks must be a multiple of the number of replications!");
		}

		this._realStateID = context.getThisTaskIndex() / _numReplications;
		this._namespace = context.getThisComponentId() + "-" + this._realStateID;
		if (context.getThisTaskIndex() % _numReplications == 0) {
			_isPrimary = true;
			initlock(_namespace);
		}
		this._state = RepStateFactory.getState(_namespace, stormConf, context);
		if (!ReplicationUtils.isRecovering(stormConf))
			_isInitialized = true;
	}

	private void initlock(String lockString) {
		_client = getClient();
		try {
			Stat stat = _client.checkExists().forPath("/" + lockString);
			if (stat == null) {
				String path = _client.create().creatingParentsIfNeeded().forPath("/" + lockString);
				LOG.info("Created: " + path);
			}
		} catch (Exception e) {
			LOG.info("Lock initialization error.");
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup() {
		_bolt.cleanup();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		_bolt.declareOutputFields(declarer);
		declareReplicationStream(declarer);

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return _bolt.getComponentConfiguration();
	}

	@Override
	protected void handleTuple(Tuple input) {
		if (_isInitialized) {
			_bolt.execute(input);
			_collector.ack(input);
		} else {
			LOG.debug("Bolt state not initialized, adding tuple {} to pending tuples", input);
			_pendingTuples.add(input);

		}
	}

	@Override
	protected void handleReplication(Tuple input, long txid) {
		LOG.debug("handle Replication with txid {}", txid);
		CuratorFramework client = getClient();
		if (!_isInitialized) {
			// load state
			_state.load(txid);
			_isInitialized = true;
			LOG.debug("{} pending tuples to process", _pendingTuples.size());
			for (Tuple tuple : _pendingTuples) {
				_bolt.execute(tuple);
				_collector.ack(tuple);
			}
			_pendingTuples.clear();

		} else {
			// save states;
			InterProcessMutex sharedLock = new InterProcessMutex(client, "/" + _namespace);
			try {
				if (sharedLock.acquire(50, TimeUnit.MILLISECONDS)) {
					_state.save(txid);
				} else {
					LOG.debug("{} cannot get the shared lock as it has been occupied.",
							Thread.currentThread().getName());
				}
			} catch (Exception e) {
				LOG.info("{} cannot get the shared lock due to error!", Thread.currentThread().getName());
				e.printStackTrace();
			} finally {
				try {
					if (sharedLock.isAcquiredInThisProcess())
						sharedLock.release();
				} catch (Exception e) {
					e.printStackTrace();
				}
				// CloseableUtils.closeQuietly(_client);
			}

		}
		_collector.delegate.emit(ReplicationSpout.REPLICATION_STREAM_ID, input, new Values(txid));
		_collector.ack(input);

	}

	private static CuratorFramework getClient() {
		if (_client == null) {
			CuratorFramework clientinit = CuratorFrameworkFactory.builder().namespace(ReplicationUtils.LOCK_NAMESPACE)
					.connectString(_connectString).retryPolicy(new RetryNTimes(_retryCount, _retryInterval)).build();
			clientinit.start();
			_client = clientinit;
		}

		return _client;
	}

	private synchronized void prepareConectionConfig(Map stormConf) {
		ReplicationStatefulBoltExecutor._connectString = zkHosts(stormConf);
		ReplicationStatefulBoltExecutor._retryCount = Utils.getInt(stormConf.get("storm.zookeeper.retry.times"));
		ReplicationStatefulBoltExecutor._retryInterval = Utils.getInt(stormConf.get("storm.zookeeper.retry.interval"));
	}

	private static String zkHosts(Map conf) {
		int zkPort = Utils.getInt(conf.get("storm.zookeeper.port"));
		List<String> zkServers = (List<String>) conf.get("storm.zookeeper.servers");

		Iterator<String> it = zkServers.iterator();
		StringBuffer sb = new StringBuffer();
		while (it.hasNext()) {
			sb.append(it.next());
			sb.append(":");
			sb.append(zkPort);
			if (it.hasNext()) {
				sb.append(",");
			}
		}
		return sb.toString();
	}


	private static class SelectiveEmitOutputCollector extends AnchoringOutputCollector {
		private final OutputCollector delegate;
		private final boolean isPrimary;
		private static final Logger LOG = LoggerFactory.getLogger(SelectiveEmitOutputCollector.class);

		SelectiveEmitOutputCollector(OutputCollector delegate, boolean isPrimary) {
			super(delegate);
			this.delegate = delegate;
			this.isPrimary = isPrimary;

		}

		@Override
		public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
			if (isPrimary)
				return delegate.emit(streamId, anchors, tuple);
			else {
				return null;
			}
		}

		@Override
		public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
			if (isPrimary)
				delegate.emitDirect(taskId, streamId, anchors, tuple);
			else {
				return;
			}
		}

	}

}