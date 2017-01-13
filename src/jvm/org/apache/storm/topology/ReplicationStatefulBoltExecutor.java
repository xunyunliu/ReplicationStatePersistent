package org.apache.storm.topology;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
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
	private AnchoringOutputCollector _collector;
	private Map _stormConf;

	private int _numTasks;
	private int _numReplications;
	private int _realTaskID;
	private String _componentName;

	private static CuratorFramework _client;

	public ReplicationStatefulBoltExecutor(IRepStatefulBolt<T> bolt) {
		_bolt = bolt;
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		init(context, collector);
		this._collector = new AnchoringOutputCollector(collector);
		_bolt.prepare(stormConf, context, this._collector);
		this._stormConf = stormConf;
		this._componentName = context.getThisComponentId();
		this._numTasks = getNumTasks(context);
		this._numReplications = loadNumReplications(stormConf);
		if (_numTasks % _numReplications != 0) {
			throw new IllegalArgumentException("The number of tasks must be a multiple of the number of Replications!");
		}
		this._realTaskID = context.getThisTaskIndex() / _numReplications;

		String namespace = context.getThisComponentId() + "-" + this._realTaskID;
		this._state = RepStateFactory.getState(namespace, stormConf, context);
		_bolt.initState((T) _state);
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
		_bolt.execute(input);
	}

	@Override
	protected void handleReplication(Tuple input, long txid) {
		LOG.debug("handle Replication with txid {}", txid);
		CuratorFramework client = getClient();
		String lockName = txid+"_"+_componentName+"_"+_realTaskID;
		try {
			iniClient(lockName);
		} catch (Exception e1) {
			
			e1.printStackTrace();
		}
		InterProcessMutex sharedLock = new InterProcessMutex(client, "/" + lockName);
		try {

			if (sharedLock.acquire(50, TimeUnit.MILLISECONDS)) {
				// sharedLock.acquire();
				LOG.info("{} has got the shared lock", Thread.currentThread().getName());
				Utils.sleep(1000);
				_state.save(txid);
				LOG.info("{} is releasing the shared lock", Thread.currentThread().getName());
			}

		} catch (Exception e) {
			LOG.info("{} cannot get the shared lock as it has been occupied", Thread.currentThread().getName());

		} finally {
			try {
				LOG.info("{} has a flag {}", Thread.currentThread().getName(), sharedLock.isAcquiredInThisProcess());
				if (sharedLock.isAcquiredInThisProcess())
					sharedLock.release();
			} catch (Exception e) {
				e.printStackTrace();
			}
			CloseableUtils.closeQuietly(_client);
			_client = null;

		}
		_collector.emit(ReplicationSpout.REPLICATION_STREAM_ID, input, new Values(txid));
		_collector.ack(input);

	}

	private void iniClient(String lockName) throws Exception {
		Stat stat = this._client.checkExists().forPath("/" + lockName);
		if (stat == null) {
			String path = this._client.create().creatingParentsIfNeeded()
					.forPath("/" + lockName);
			LOG.info("Created: " + path);
		}
	}

	private CuratorFramework getClient() {
		if (_client == null) {

			String connectString = zkHosts(_stormConf);
			int retryCount = Utils.getInt(_stormConf.get("storm.zookeeper.retry.times"));
			int retryInterval = Utils.getInt(_stormConf.get("storm.zookeeper.retry.interval"));

			CuratorFramework clientinit = CuratorFrameworkFactory.builder().namespace(ReplicationUtils.LOCK_NAMESPACE)
					.connectString(connectString).retryPolicy(new RetryNTimes(retryCount, retryInterval)).build();
			clientinit.start();
			_client = clientinit;
		}

		return _client;
	}

	private String zkHosts(Map conf) {
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

	private int getNumTasks(TopologyContext context) {
		return context.getComponentTasks(context.getThisComponentId()).size();
	}

	private int loadNumReplications(Map stormConf) {
		int numReplications = 0;
		if (stormConf.containsKey(ReplicationUtils.TOPOLOGY_NUMREPLICATIONS)) {
			numReplications = ((Number) stormConf.get(ReplicationUtils.TOPOLOGY_NUMREPLICATIONS)).intValue();
		}
		numReplications = Math.max(1, numReplications);
		LOG.info("The global number of replications is {}.", numReplications);
		return numReplications;
	}

}