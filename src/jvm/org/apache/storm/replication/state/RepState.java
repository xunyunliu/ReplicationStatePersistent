package org.apache.storm.replication.state;

public interface RepState {

	void save(long txid); 
}
