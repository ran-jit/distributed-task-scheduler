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
	 * To add data with score to data-cache
	 * 
	 * @param key
	 * @param data
	 * @param score
	 */
	void add(String key, Object data, double score);

	/**
	 * To check data exists in data-cache
	 * 
	 * @param key
	 * @param data
	 * @return
	 */
	Boolean exists(String key, Object data);

	/**
	 * To remove data from data-cache
	 * 
	 * @param key
	 * @param data
	 * @return
	 */
	Long remove(String key, Object... data);

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
	Set<Object> get(String key, double minScore, double maxScore, int offset, int count);

	/**
	 * To get data score from data-cache
	 * 
	 * @param key
	 * @param data
	 * @return
	 */
	Double getScore(String key, Object data);
}