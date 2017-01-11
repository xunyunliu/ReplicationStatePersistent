package org.apache.storm.topology;

import java.util.Map;

import org.apache.storm.replication.state.RepState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

public interface IRepStatefulBolt <T extends RepState> extends IRichBolt {
	
    /**
     * @see org.apache.storm.task.IBolt#prepare(Map, TopologyContext, OutputCollector)
     */
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);
    /**
     * @see org.apache.storm.task.IBolt#execute(Tuple)
     */
    void execute(Tuple input);
    /**
     * @see org.apache.storm.task.IBolt#cleanup()
     */
    void cleanup();

    /**
     * This method is invoked by the framework with the previously
     * saved state of the component. This is invoked after prepare but before
     * the component starts processing tuples.
     *
     * @param state the previously saved state of the component.
     */
    void initState(T state);
}
