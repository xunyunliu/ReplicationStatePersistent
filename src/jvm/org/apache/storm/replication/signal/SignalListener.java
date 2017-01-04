package org.apache.storm.replication.signal;

public interface SignalListener {

	 void onSignal(byte[] data);
}
