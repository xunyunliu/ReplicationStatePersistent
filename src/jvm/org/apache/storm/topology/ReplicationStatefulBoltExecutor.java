package org.apache.storm.topology;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.storm.state.State;
import org.apache.storm.state.StateFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationStatefulBoltExecutor<T extends State> extends BaseReplicationBoltExecutor {

	private static final Logger LOG = LoggerFactory.getLogger(ReplicationStatefulBoltExecutor.class);
	private final IStatefulBolt<T> bolt;
	private State state;
	private boolean boltInitialized = false;
	private List<Tuple> pendingTuples = new ArrayList<>();
	private List<Tuple> preparedTuples = new ArrayList<>();
	private AckTrackingOutputCollector collector;

	public ReplicationStatefulBoltExecutor(IStatefulBolt<T> bolt) {
		this.bolt = bolt;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		String namespace = context.getThisComponentId() + "-" + context.getThisTaskId();
		prepare(stormConf, context, collector, StateFactory.getState(namespace, stormConf, context));
	}

	private void prepare(Map stormConf, TopologyContext context, OutputCollector collector, State state) {
		init(context, collector);
		this.collector = new AckTrackingOutputCollector(collector);
		bolt.prepare(stormConf, context, this.collector);
		this.state = state;
	}

	@Override
	public void cleanup() {
		bolt.cleanup();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		bolt.declareOutputFields(declarer);
		declareReplicationStream(declarer);

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return bolt.getComponentConfiguration();
	}

	@Override
	protected void handleTuple(Tuple input) {
		if (boltInitialized) {
			doExecute(input);
		} else {
			LOG.debug("Bolt state not initialized, adding tuple {} to pending tuples", input);
			pendingTuples.add(input);
		}
	}

	private void doExecute(Tuple tuple) {
		bolt.execute(tuple);
	}

	private void ack(List<Tuple> tuples) {
		if (!tuples.isEmpty()) {
			LOG.debug("Acking {} tuples", tuples.size());
			for (Tuple tuple : tuples) {
				collector.delegate.ack(tuple);
			}
			tuples.clear();
		}
	}

	private void fail(List<Tuple> tuples) {
		if (!tuples.isEmpty()) {
			LOG.debug("Failing {} tuples", tuples.size());
			for (Tuple tuple : tuples) {
				collector.fail(tuple);
			}
			tuples.clear();
		}
	}

	@Override
	protected void handleReplication(Tuple input, long txid) {
		// TODO Auto-generated method stub

	}
	
	 private static class AckTrackingOutputCollector extends AnchoringOutputCollector {
	        private final OutputCollector delegate;
	        private final Queue<Tuple> ackedTuples;

	        AckTrackingOutputCollector(OutputCollector delegate) {
	            super(delegate);
	            this.delegate = delegate;
	            this.ackedTuples = new ConcurrentLinkedQueue<>();
	        }

	        List<Tuple> ackedTuples() {
	            List<Tuple> result = new ArrayList<>();
	            Iterator<Tuple> it = ackedTuples.iterator();
	            while(it.hasNext()) {
	                result.add(it.next());
	                it.remove();
	            }
	            return result;
	        }

	        @Override
	        public void ack(Tuple input) {
	            ackedTuples.add(input);
	        }
	    }

}
