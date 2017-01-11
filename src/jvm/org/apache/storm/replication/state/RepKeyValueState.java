package org.apache.storm.replication.state;

public interface RepKeyValueState <K,V> extends RepState{

	 /**
     * Maps the value with the key
     *
     * @param key   the key
     * @param value the value
     */
    void put(K key, V value);

    /**
     * Returns the value mapped to the key
     *
     * @param key the key
     * @return the value or null if no mapping is found
     */
    V get(K key);

    /**
     * Returns the value mapped to the key or defaultValue if no mapping is found.
     *
     * @param key          the key
     * @param defaultValue the value to return if no mapping is found
     * @return the value or defaultValue if no mapping is found
     */
    V get(K key, V defaultValue);
}
