package org.apache.storm.topology;

import static org.apache.storm.spout.ReplicationSpout.REPLICATION_STREAM_ID;
import static org.apache.storm.spout.ReplicationSpout.REPLICATION_FLELD_ID;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.spout.ReplicationSpout;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseReplicationBoltExecutor implements IRichBolt {

	private static final Logger LOG = LoggerFactory.getLogger(BaseReplicationBoltExecutor.class);
	protected OutputCollector _collector;
	private final Map<Long, Integer> _transactionRequestCount;
	private int _replicationInputTaskCount;

	public BaseReplicationBoltExecutor() {
		_transactionRequestCount = new HashMap<>();
	}

	protected void init(TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_replicationInputTaskCount = getReplicationInputTaskCount(context);
	}

	/**
	 * returns the total number of input replication streams across all input
	 * tasks to this component.
	 */
	private int getReplicationInputTaskCount(TopologyContext context) {
		int count = 0;
		for (GlobalStreamId inputStream : context.getThisSources().keySet()) {
			if (REPLICATION_STREAM_ID.equals(inputStream.get_streamId())) {
				count += context.getComponentTasks(inputStream.get_componentId()).size();
			}
		}
		return count;
	}

	@Override
	public void execute(Tuple input) {
		if (ReplicationSpout.isReplication(input)) {
			processReplication(input);
		} else {
			handleTuple(input);
		}
	}

	private void processReplication(Tuple input) {
		long txid = input.getLongByField(REPLICATION_FLELD_ID);
		if (shouldProcessTransaction(txid)) {
			LOG.debug("Processing replication message, txid {}", txid);
			try {
				// need to ack this input;
				handleReplication(input, txid);
			} catch (Throwable th) {
				LOG.error("Got error while processing replication tuple", th);
				_collector.fail(input);
				_collector.reportError(th);
			}
		} else {
			LOG.debug("Waiting for txid {} from all input tasks. replicationInputTaskCount {}, "
					+ "transactionRequestCount {}", txid, _replicationInputTaskCount, _transactionRequestCount);
			_collector.ack(input);
		}

	}

	private boolean shouldProcessTransaction(long txid) {
		Long request = new Long(txid);
		Integer count;
		if ((count = _transactionRequestCount.get(request)) == null) {
			_transactionRequestCount.put(request, 1);
			count = 1;
		} else {
			_transactionRequestCount.put(request, ++count);
		}
		if (count == _replicationInputTaskCount) {
			_transactionRequestCount.remove(request);
			return true;
		}
		return false;
	}

	/**
	 * Sub-classes can implement the logic for handling the tuple.
	 *
	 * @param input:
	 *            the input tuple
	 */
	protected abstract void handleTuple(Tuple input);

	/**
	 * Sub-classes can implement the logic for handling replication tuple.
	 * 
	 * @param input
	 */
	protected abstract void handleReplication(Tuple input, long txid);

	protected void declareReplicationStream(OutputFieldsDeclarer declarer) {
		declarer.declareStream(ReplicationSpout.REPLICATION_STREAM_ID,
				new Fields(ReplicationSpout.REPLICATION_FLELD_ID));
	}

	protected static class AnchoringOutputCollector extends OutputCollector {
		AnchoringOutputCollector(IOutputCollector delegate) {
			super(delegate);
		}

		@Override
		public List<Integer> emit(String streamId, List<Object> tuple) {
			throw new UnsupportedOperationException("Bolts in a stateful topology must emit anchored tuples.");
		}

		@Override
		public void emitDirect(int taskId, String streamId, List<Object> tuple) {
			throw new UnsupportedOperationException("Bolts in a stateful topology must emit anchored tuples.");
		}
	}

}
