package org.apache.storm.replication.signal;

public interface SignalListener {

	/**
	 * Perform some user-defined action when receiving the signal
	 * @param data
	 */
	void onSignal(byte[] data);
}
