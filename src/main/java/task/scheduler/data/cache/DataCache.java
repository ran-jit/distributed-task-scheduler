package task.scheduler.data.cache;

import java.util.Set;

/**
 * Distributed Task scheduler implementation.
 * 
 * Interface to add/modify/retrieve data objects in data-cache.
 * 
 * @author Ranjith Manickam
 * @since 1.0
 */
public interface DataCache {

	/**
	 * To add task scheduler name to data-cache
	 * 
	 * @param key
	 * @param value
	 */
	void sAdd(String key, String value);

	/**
	 * To list task scheduler names from data-cache
	 * 
	 * @param key
	 * @return
	 */
	Set<String> sMembers(String key);

	/**
	 * To add data with score to data-cache
	 * 
	 * @param key
	 * @param data
	 * @param score
	 */
	void zAdd(String key, Object data, double score);

	/**
	 * To check data exists in data-cache
	 * 
	 * @param key
	 * @param data
	 * @return
	 */
	Boolean zExists(String key, Object data);

	/**
	 * To remove data from data-cache
	 * 
	 * @param key
	 * @param data
	 * @return
	 */
	Long zRemove(String key, Object... data);

	/**
	 * To get data from data-cache based on score, offset and count
	 * 
	 * @param key
	 * @param minScore
	 * @param maxScore
	 * @param offset
	 * @param count
	 * @return
	 */
	Set<Object> zGet(String key, double minScore, double maxScore, int offset, int count);

	/**
	 * To get data score from data-cache
	 * 
	 * @param key
	 * @param data
	 * @return
	 */
	Double zScore(String key, Object data);

	/**
	 * To get data count from data-cache
	 * 
	 * @param key
	 * @return
	 */
	Long zCount(String key);
}