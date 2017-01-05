package org.apache.storm.utils;

import java.util.Map;

public class ReplicationUtils {
	public final static String TOPO_START_TIME = "topo-start-time";
	public final static int TOPO_INITIALIZATION_TIME = 20;
	public final static int TOPO_RECOVERY_TIMEOUT = 300;

	@SuppressWarnings("rawtypes")
	public static long getTopologyUpTime(Map Stormconf) {
		if (!Stormconf.containsKey(TOPO_START_TIME))
			throw new IllegalStateException("The topology start time has not been set!");

		long topoStartTime = (Long) Stormconf.get("topo-start-time");
		long uptime = (System.currentTimeMillis() - topoStartTime) / 1000;

		return uptime;

	}

	@SuppressWarnings("rawtypes")
	public static boolean isRecovering(Map Stormconf) {
		return getTopologyUpTime(Stormconf) >= TOPO_INITIALIZATION_TIME ? true : false;
	}

}
