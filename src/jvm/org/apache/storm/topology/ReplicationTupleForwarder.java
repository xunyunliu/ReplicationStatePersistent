package org.apache.storm.topology;

import static org.apache.storm.spout.ReplicationSpout.REPLICATION_STREAM_ID;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BaseStatefulBoltExecutor.AnchoringOutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps {@link IRichBolt} and forwards replication tuples in a stateful
 * topology.
 * <p>
 * When a storm topology contains one or more {@link IStatefulBolt} all
 * non-stateful bolts are wrapped in {@link ReplicationTupleForwarder} so that
 * the replication tuples can flow through the entire topology DAG.
 * </p>
 */
public class ReplicationTupleForwarder extends BaseReplicationBoltExecutor {
	private static final Logger LOG = LoggerFactory.getLogger(ReplicationTupleForwarder.class);
	private final IRichBolt _bolt;

	public ReplicationTupleForwarder(IRichBolt bolt) {
		this._bolt = bolt;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		init(context, new AnchoringOutputCollector(collector));
		_bolt.prepare(stormConf, context, _collector);
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

	}

	@Override
	protected void handleReplication(Tuple input, long txid) {
		_collector.emit(REPLICATION_STREAM_ID, input, new Values(txid));
		_collector.ack(input);
	}

}
