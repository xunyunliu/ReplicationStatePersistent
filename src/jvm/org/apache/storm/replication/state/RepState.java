package org.apache.storm.replication.state;

public interface RepState {

	void load (long txid);
	void save(long txid); 
}
