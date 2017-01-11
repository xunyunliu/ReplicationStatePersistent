package org.apache.storm.topology;

import static org.apache.storm.spout.ReplicationSpout.REPLICATION_STREAM_ID;
import static org.apache.storm.spout.ReplicationSpout.REPLICATION_FLELD_ID;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.spout.ReplicationSpout;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class that abstracts the common logic for executing bolts in a stateful
 * topology with replication.
 * 
 * @author xunyunliu
 *
 */
public abstract class BaseReplicationBoltExecutor implements IRichBolt {

	private static final Logger LOG = LoggerFactory.getLogger(BaseReplicationBoltExecutor.class);
	private Set<Long> _transactionProcessed;
	protected OutputCollector _collector;

	public BaseReplicationBoltExecutor() {
		_transactionProcessed = new HashSet<Long>();
	}

	protected void init(TopologyContext context, OutputCollector collector) {
		_collector = collector;
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
			LOG.debug("Processing replication signal, txid {}", txid);
			try {
				// need to ack this input;
				handleReplication(input, txid);
			} catch (Throwable th) {
				LOG.error("Got error while processing replication signal", th);
				_collector.fail(input);
				_collector.reportError(th);
			}
		} else {
			LOG.debug("This txid {} has already been processed", txid);
			_collector.ack(input);
		}

	}

	private boolean shouldProcessTransaction(long txid) {
		if (_transactionProcessed.contains(txid))
			return false;
		else {
			_transactionProcessed.add(txid);
			return true;
		}
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
		declarer.declareStream(REPLICATION_STREAM_ID, new Fields(REPLICATION_FLELD_ID));
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