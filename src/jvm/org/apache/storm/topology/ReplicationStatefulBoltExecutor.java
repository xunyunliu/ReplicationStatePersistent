package org.apache.storm.topology;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.replication.state.RepState;
import org.apache.storm.replication.state.RepStateFactory;
import org.apache.storm.spout.ReplicationSpout;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationStatefulBoltExecutor<T extends RepState> extends BaseReplicationBoltExecutor {

	private static final Logger LOG = LoggerFactory.getLogger(ReplicationStatefulBoltExecutor.class);
	private final IRepStatefulBolt<T> _bolt;
	private RepState _state;
	private AnchoringOutputCollector _collector;
	private int _numTasks;
	private int _numReplications;
	private int _realTaskID;

	public ReplicationStatefulBoltExecutor(IRepStatefulBolt<T> bolt) {
		_bolt = bolt;
	}



	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		init(context, collector);
		this._collector = new AnchoringOutputCollector(collector);
		_bolt.prepare(stormConf, context, this._collector);

		this._numTasks = getNumTasks(context);
		this._numReplications = loadNumReplications(stormConf);
		if (_numTasks % _numReplications != 0) {
			throw new IllegalArgumentException("The number of tasks must be a multiple of the number of Replications!");
		}
		this._realTaskID = context.getThisTaskIndex() / _numReplications;

		String namespace = context.getThisComponentId() + "-" + this._realTaskID;
		this._state = RepStateFactory.getState(namespace, stormConf, context);
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
		_state.save(txid);
		_collector.emit(ReplicationSpout.REPLICATION_STREAM_ID, input, new Values(txid));
		_collector.ack(input);

	}

	private int getNumTasks(TopologyContext context) {
		return context.getComponentTasks(context.getThisComponentId()).size();
	}

	private int loadNumReplications(Map stormConf) {
		int numReplications = 0;
		if (stormConf.containsKey(Config.TOPOLOGY_NUMREPLICATIONS)) {
			numReplications = ((Number) stormConf.get(Config.TOPOLOGY_NUMREPLICATIONS)).intValue();
		}
		numReplications = Math.max(1, numReplications);
		LOG.info("The global number of replications is {} .", numReplications);
		return numReplications;
	}
	
}