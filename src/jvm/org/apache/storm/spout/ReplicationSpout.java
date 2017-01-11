package org.apache.storm.spout;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;

/**
 * The replication spout is used to send recovery signals. Upon receiving the
 * message, the downstream bolts will migrate their states to the siblings that
 * have suffered from failures.
 * 
 * @author xunyunliu
 *
 */
public class ReplicationSpout extends BaseSignalSpout {

	private static final Logger LOG = LoggerFactory.getLogger(ReplicationSpout.class);

	public static final String REPLICATION_STREAM_ID = "$replication";
	public static final String REPLICATION_COMPONENT_ID = "replicationspout";
	public static final String REPLICATION_FLELD_ID = "txid";

	/**
	 * the unique id representing the replication message being transmitted
	 * across the topology.
	 */
	private long _curReplicationTxID;

	private TopologyContext _context;
	private SpoutOutputCollector _collector;
	private int _sleepInterval;

	public ReplicationSpout(String name) {
		super(name);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		_context = context;
		_collector = collector;
		_curReplicationTxID = 0;
		_sleepInterval = 100;
		// LOG.info("Collector class: " + collector.getClass().getName());

	}

	@Override
	public void ack(Object msgId) {
		LOG.info("Ack received, replication singal {} has been fully processed.", _curReplicationTxID);
		_curReplicationTxID = 0;
	}

	@Override
	public void fail(Object msgId) {
		LOG.info("Replication signal {} failed as it times out. Resending signal required", _curReplicationTxID);
	}

	@Override
	public void onSignal(byte[] data) {
		LOG.info("Received replication signal: " + new String(data));
		if (_curReplicationTxID != 0)
			LOG.info("There is an existing signal {} waiting to be processed by the topology. New signal is ignored",
					_curReplicationTxID);

		try {
			_curReplicationTxID = Long.parseLong(new String(data));
		} catch (NumberFormatException nfe) {
			LOG.info("Signal should be a number as it represents the id of replication message.");
		}

		_collector.emit(REPLICATION_STREAM_ID, new Values(_curReplicationTxID), _curReplicationTxID);
		LOG.info("New replication message with ID {} has been sent.", _curReplicationTxID);

	}

	/**
	 * Examples of reading Storm configurations.
	 * 
	 * @param stormConf
	 * @return
	 */
	@SuppressWarnings("unused")
	private int loadCheckpointInterval(Map stormConf) {
		int interval = 0;
		if (stormConf.containsKey(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL)) {
			interval = ((Number) stormConf.get(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL)).intValue();
		}
		// ensure checkpoint interval is not less than a sane low value.
		interval = Math.max(100, interval);
		LOG.info("Checkpoint interval is {} millis", interval);
		return interval;
	}

	@Override
	public void nextTuple() {
		Utils.sleep(_sleepInterval);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(REPLICATION_STREAM_ID, new Fields(REPLICATION_FLELD_ID));
	}

	public static boolean isReplication(Tuple input) {
		return REPLICATION_STREAM_ID.equals(input.getSourceGlobalStreamId());
	}

}