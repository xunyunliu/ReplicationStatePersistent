package org.apache.storm.utils;

import java.util.Map;

import org.apache.storm.task.GeneralTopologyContext;


public class ReplicationUtils {
	public final static String TOPO_START_TIME = "topo-start-time";
	public final static String LOCK_NAMESPACE = "lock-namespace";
	public final static int TOPO_INITIALIZATION_TIME = 20;
	public final static String TOPOLOGY_NUMREPLICATIONS = "topology.numreplications";


	@SuppressWarnings("rawtypes")
	public static boolean isRecovering(Map Stormconf) {
		return getTopologyUpTime(Stormconf) >= TOPO_INITIALIZATION_TIME ? true : false;
	}
	
	@SuppressWarnings("rawtypes")
	public static int loadNumReplications(GeneralTopologyContext context) {
		int numReplications = 0;
		Map stormConf = context.getStormConf();
		if (stormConf.containsKey(ReplicationUtils.TOPOLOGY_NUMREPLICATIONS)) {
			numReplications = ((Number) stormConf.get(ReplicationUtils.TOPOLOGY_NUMREPLICATIONS)).intValue();
		}
		numReplications = Math.max(1, numReplications);
		return numReplications;
	}

	

	@SuppressWarnings("rawtypes")
	public static long getTopologyUpTime(Map Stormconf) {
		if (!Stormconf.containsKey(TOPO_START_TIME))
			throw new IllegalStateException("The topology start time has not been set!");

		long topoStartTime = (Long) Stormconf.get("topo-start-time");
		long uptime = (System.currentTimeMillis() - topoStartTime) / 1000;

		return uptime;

	}

}
