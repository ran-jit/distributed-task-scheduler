package task.scheduler.data.cache.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisClusterMaxRedirectionsException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import task.scheduler.constants.TaskSchedulerConstants;
import task.scheduler.data.cache.DataCache;
import task.scheduler.gui.TaskTrackerHandler;

/**
 * Distributed Task scheduler implementation.
 * 
 * Redis data-cache for add/modify/retrieve data objects.
 * 
 * @author Ranjith Manickam
 * @since 1.0
 */
public class RedisDataCache implements DataCache {

	private static DataCache dataCache;

	private Log log = LogFactory.getLog(RedisDataCache.class);

	public RedisDataCache() {
		initialize(null, null);
	}

	public RedisDataCache(String filePath) {
		initialize(null, filePath);
	}

	public RedisDataCache(Properties properties) {
		initialize(properties, null);
	}

	/** {@inheritDoc} */
	@Override
	public void sAdd(String key, String value) {
		dataCache.sAdd(parseDataCacheKey(key), value);
	}

	/** {@inheritDoc} */
	@Override
	public Set<String> sMembers(String key) {
		return dataCache.sMembers(parseDataCacheKey(key));
	}

	/** {@inheritDoc} */
	@Override
	public void zAdd(String key, Object data, double score) {
		dataCache.zAdd(parseDataCacheKey(key), data, score);
	}

	/** {@inheritDoc} */
	@Override
	public Boolean zExists(String key, Object data) {
		return dataCache.zExists(parseDataCacheKey(key), data);
	}

	/** {@inheritDoc} */
	@Override
	public Long zRemove(String key, Object... data) {
		return dataCache.zRemove(parseDataCacheKey(key), data);
	}

	/** {@inheritDoc} */
	@Override
	public Set<Object> zGet(String key, double minScore, double maxScore, int offset, int count) {
		return dataCache.zGet(parseDataCacheKey(key), minScore, maxScore, offset, count);
	}

	/** {@inheritDoc} */
	@Override
	public Double zScore(String key, Object data) {
		return dataCache.zScore(parseDataCacheKey(key), data);
	}

	/** {@inheritDoc} */
	@Override
	public Long zCount(String key) {
		return dataCache.zCount(parseDataCacheKey(key));
	}

	/**
	 * method to parse data-cache key
	 * 
	 * @param key
	 * @return
	 */
	public static String parseDataCacheKey(String key) {
		return key.replaceAll("\\s", "_");
	}

	/**
	 * method to initialize the data-cache
	 * 
	 * @param properties
	 * @param filePath
	 */
	@SuppressWarnings("unchecked")
	private void initialize(Properties properties, String filePath) {
		if (dataCache != null) {
			return;
		}
		properties = (properties == null) ? loadProperties(filePath) : properties;

		boolean clusterEnabled = Boolean.valueOf(properties.getProperty(RedisConstants.CLUSTER_ENABLED, RedisConstants.DEFAULT_CLUSTER_ENABLED));

		String hosts = properties.getProperty(RedisConstants.HOSTS, Protocol.DEFAULT_HOST.concat(":").concat(String.valueOf(Protocol.DEFAULT_PORT)));
		Collection<? extends Serializable> nodes = getJedisNodes(hosts, clusterEnabled);

		String password = properties.getProperty(RedisConstants.PASSWORD);
		password = StringUtils.isNotBlank(password) ? password : null;

		int database = Integer.parseInt(properties.getProperty(RedisConstants.DATABASE, String.valueOf(Protocol.DEFAULT_DATABASE)));

		int timeout = Integer.parseInt(properties.getProperty(RedisConstants.TIMEOUT, String.valueOf(Protocol.DEFAULT_TIMEOUT)));
		timeout = (timeout < Protocol.DEFAULT_TIMEOUT) ? Protocol.DEFAULT_TIMEOUT : timeout;

		if (clusterEnabled) {
			dataCache = new RedisClusterCacheUtil((Set<HostAndPort>) nodes, timeout, getPoolConfig(properties));
		} else {
			dataCache = new RedisCacheUtil(((List<String>) nodes).get(0),
					Integer.parseInt(((List<String>) nodes).get(1)), password, database, timeout, getPoolConfig(properties));
		}

		boolean guiEnabled = Boolean.valueOf(properties.getProperty(TaskSchedulerConstants.IS_GUI_ENABLED, RedisConstants.DEFAULT_CLUSTER_ENABLED));
		if (guiEnabled) {
			int port = Integer.parseInt(properties.getProperty(TaskSchedulerConstants.GUI_PORT,
					String.valueOf(TaskSchedulerConstants.DEFAULT_SCHEDULER_GUI_PORT)));
			new TaskTrackerHandler().startServer(port);
		}
	}

	/**
	 * method to get jedis pool configuration
	 * 
	 * @param properties
	 * @return
	 */
	private JedisPoolConfig getPoolConfig(Properties properties) {
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		int maxActive = Integer.parseInt(properties.getProperty(RedisConstants.MAX_ACTIVE, RedisConstants.DEFAULT_MAX_ACTIVE_VALUE));
		poolConfig.setMaxTotal(maxActive);

		boolean testOnBorrow = Boolean.parseBoolean(properties.getProperty(RedisConstants.TEST_ONBORROW, RedisConstants.DEFAULT_TEST_ONBORROW_VALUE));
		poolConfig.setTestOnBorrow(testOnBorrow);

		boolean testOnReturn = Boolean.parseBoolean(properties.getProperty(RedisConstants.TEST_ONRETURN, RedisConstants.DEFAULT_TEST_ONRETURN_VALUE));
		poolConfig.setTestOnReturn(testOnReturn);

		int maxIdle = Integer.parseInt(properties.getProperty(RedisConstants.MAX_ACTIVE, RedisConstants.DEFAULT_MAX_ACTIVE_VALUE));
		poolConfig.setMaxIdle(maxIdle);

		int minIdle = Integer.parseInt(properties.getProperty(RedisConstants.MIN_IDLE, RedisConstants.DEFAULT_MIN_IDLE_VALUE));
		poolConfig.setMinIdle(minIdle);

		boolean testWhileIdle = Boolean.parseBoolean(properties.getProperty(RedisConstants.TEST_WHILEIDLE, RedisConstants.DEFAULT_TEST_WHILEIDLE_VALUE));
		poolConfig.setTestWhileIdle(testWhileIdle);

		int testNumPerEviction = Integer.parseInt(properties.getProperty(RedisConstants.TEST_NUMPEREVICTION, RedisConstants.DEFAULT_TEST_NUMPEREVICTION_VALUE));
		poolConfig.setNumTestsPerEvictionRun(testNumPerEviction);

		long timeBetweenEviction = Long.parseLong(properties.getProperty(RedisConstants.TIME_BETWEENEVICTION, RedisConstants.DEFAULT_TIME_BETWEENEVICTION_VALUE));
		poolConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEviction);
		return poolConfig;
	}

	/**
	 * method to get jedis nodes
	 * 
	 * @param hosts
	 * @param clusterEnabled
	 * @return
	 */
	private Collection<? extends Serializable> getJedisNodes(String hosts, boolean clusterEnabled) {
		hosts = hosts.replaceAll("\\s", "");
		String[] hostPorts = hosts.split(",");

		List<String> node = null;
		Set<HostAndPort> nodes = null;

		for (String hostPort : hostPorts) {
			String[] hostPortArr = hostPort.split(":");

			if (clusterEnabled) {
				nodes = (nodes == null) ? new HashSet<HostAndPort>() : nodes;
				nodes.add(new HostAndPort(hostPortArr[0], Integer.valueOf(hostPortArr[1])));
			} else {
				int port = Integer.valueOf(hostPortArr[1]);
				if (!hostPortArr[0].isEmpty() && port > 0) {
					node = (node == null) ? new ArrayList<String>() : node;
					node.add(hostPortArr[0]);
					node.add(String.valueOf(port));
					break;
				}
			}
		}
		return clusterEnabled ? nodes : node;
	}

	/**
	 * method to load data-cache properties
	 * 
	 * @param filePath
	 * @return
	 */
	private Properties loadProperties(String filePath) {
		Properties properties = new Properties();
		try {
			InputStream resourceStream = null;
			try {
				resourceStream = (StringUtils.isNotBlank(filePath) && new File(filePath).exists())
						? new FileInputStream(filePath) : null;

				if (resourceStream == null) {
					ClassLoader loader = Thread.currentThread().getContextClassLoader();
					resourceStream = loader.getResourceAsStream(TaskSchedulerConstants.PROPERTIES_FILE);
				}
				properties.load(resourceStream);
			} finally {
				resourceStream.close();
			}
		} catch (IOException ex) {
			log.error("Error while loading task scheduler properties", ex);
		}
		return properties;
	}

	/**
	 * Distributed Task scheduler implementation.
	 * 
	 * Redis stand-alone mode data-cache implementation.
	 * 
	 * @author Ranjith Manickam
	 * @since 1.0
	 */
	private class RedisCacheUtil implements DataCache {

		private JedisPool pool;

		private final int numRetries = 3;

		private Log log = LogFactory.getLog(RedisCacheUtil.class);

		public RedisCacheUtil(String host, int port, String password, int database, int timeout, JedisPoolConfig poolConfig) {
			pool = new JedisPool(poolConfig, host, port, timeout, password, database);
		}

		/** {@inheritDoc} */
		@Override
		public void sAdd(String key, String value) {
			int tries = 0;
			boolean sucess = false;
			do {
				tries++;
				try {
					Jedis jedis = pool.getResource();
					jedis.sadd(key, value);
					jedis.close();
					sucess = true;
				} catch (JedisConnectionException ex) {
					log.error(RedisConstants.CONN_FAILED_RETRY_MSG + tries);
					if (tries == numRetries)
						throw ex;
				}
			} while (!sucess && tries <= numRetries);
		}

		/** {@inheritDoc} */
		@Override
		public Set<String> sMembers(String key) {
			int tries = 0;
			boolean sucess = false;
			Set<String> retVal = null;
			do {
				tries++;
				try {
					Jedis jedis = pool.getResource();
					retVal = jedis.smembers(key);
					jedis.close();
					sucess = true;
				} catch (JedisConnectionException ex) {
					log.error(RedisConstants.CONN_FAILED_RETRY_MSG + tries);
					if (tries == numRetries)
						throw ex;
				}
			} while (!sucess && tries <= numRetries);
			return retVal;
		}

		/** {@inheritDoc} */
		@Override
		public void zAdd(String key, Object data, double score) {
			int tries = 0;
			boolean sucess = false;
			do {
				tries++;
				try {
					Jedis jedis = pool.getResource();
					jedis.zadd(key, score, SerializationUtil.serialize(data));
					jedis.close();
					sucess = true;
				} catch (JedisConnectionException ex) {
					log.error(RedisConstants.CONN_FAILED_RETRY_MSG + tries);
					if (tries == numRetries)
						throw ex;
				}
			} while (!sucess && tries <= numRetries);
		}

		/** {@inheritDoc} */
		@Override
		public Boolean zExists(String key, Object data) {
			int tries = 0;
			boolean sucess = false;
			boolean retVal = false;
			do {
				tries++;
				try {
					Jedis jedis = pool.getResource();
					retVal = (jedis.zscore(key, SerializationUtil.serialize(data)) != null);
					jedis.close();
					sucess = true;
				} catch (JedisConnectionException ex) {
					log.error(RedisConstants.CONN_FAILED_RETRY_MSG + tries);
					if (tries == numRetries)
						throw ex;
				}
			} while (!sucess && tries <= numRetries);
			return retVal;
		}

		/** {@inheritDoc} */
		@Override
		public Long zRemove(String key, Object... data) {
			int tries = 0;
			Long retVal = null;
			boolean sucess = false;
			do {
				tries++;
				try {
					Jedis jedis = pool.getResource();
					retVal = jedis.zrem(key, SerializationUtil.serialize(data));
					jedis.close();
					sucess = true;
				} catch (JedisConnectionException ex) {
					log.error(RedisConstants.CONN_FAILED_RETRY_MSG + tries);
					if (tries == numRetries)
						throw ex;
				}
			} while (!sucess && tries <= numRetries);
			return retVal;
		}

		/** {@inheritDoc} */
		@Override
		public Set<Object> zGet(String key, double minScore, double maxScore, int offset, int count) {
			int tries = 0;
			boolean sucess = false;
			Set<String> retVal = null;
			do {
				tries++;
				try {
					Jedis jedis = pool.getResource();
					retVal = jedis.zrangeByScore(key, minScore, maxScore, offset, count);
					jedis.close();
					sucess = true;
				} catch (JedisConnectionException ex) {
					log.error(RedisConstants.CONN_FAILED_RETRY_MSG + tries);
					if (tries == numRetries)
						throw ex;
				}
			} while (!sucess && tries <= numRetries);
			return SerializationUtil.deSerialize(retVal);
		}

		/** {@inheritDoc} */
		@Override
		public Double zScore(String key, Object data) {
			int tries = 0;
			boolean sucess = false;
			Double retVal = null;
			do {
				tries++;
				try {
					Jedis jedis = pool.getResource();
					retVal = jedis.zscore(key, SerializationUtil.serialize(data));
					jedis.close();
					sucess = true;
				} catch (JedisConnectionException ex) {
					log.error(RedisConstants.CONN_FAILED_RETRY_MSG + tries);
					if (tries == numRetries) {
						throw ex;
					}
				}
			} while (!sucess && tries <= numRetries);
			return retVal;
		}

		/** {@inheritDoc} */
		@Override
		public Long zCount(String key) {
			int tries = 0;
			boolean sucess = false;
			Long retVal = null;
			do {
				tries++;
				try {
					Jedis jedis = pool.getResource();
					retVal = jedis.zcount(key, 0, Double.MAX_VALUE);
					jedis.close();
					sucess = true;
				} catch (JedisConnectionException ex) {
					log.error(RedisConstants.CONN_FAILED_RETRY_MSG + tries);
					if (tries == numRetries) {
						throw ex;
					}
				}
			} while (!sucess && tries <= numRetries);
			return retVal;
		}
	}

	/**
	 * Distributed Task scheduler implementation.
	 * 
	 * Redis multiple node cluster data-cache implementation.
	 * 
	 * @author Ranjith Manickam
	 * @since 1.0
	 */
	private class RedisClusterCacheUtil implements DataCache {

		private JedisCluster cluster;

		private final int numRetries = 30;

		private Log log = LogFactory.getLog(RedisClusterCacheUtil.class);

		public RedisClusterCacheUtil(Set<HostAndPort> nodes, int timeout, JedisPoolConfig poolConfig) {
			cluster = new JedisCluster(nodes, timeout, poolConfig);
		}

		/** {@inheritDoc} */
		@Override
		public void sAdd(String key, String value) {
			int tries = 0;
			boolean sucess = false;
			do {
				tries++;
				try {
					cluster.sadd(key, value);
					sucess = true;
				} catch (JedisClusterMaxRedirectionsException | JedisConnectionException ex) {
					log.error(RedisConstants.CONN_FAILED_RETRY_MSG + tries);
					if (tries == numRetries) {
						throw ex;
					}
					waitforFailover();
				}
			} while (!sucess && tries <= numRetries);
		}

		/** {@inheritDoc} */
		@Override
		public Set<String> sMembers(String key) {
			int tries = 0;
			boolean sucess = false;
			Set<String> retVal = null;
			do {
				tries++;
				try {
					retVal = cluster.smembers(key);
					sucess = true;
				} catch (JedisClusterMaxRedirectionsException | JedisConnectionException ex) {
					log.error(RedisConstants.CONN_FAILED_RETRY_MSG + tries);
					if (tries == numRetries) {
						throw ex;
					}
					waitforFailover();
				}
			} while (!sucess && tries <= numRetries);
			return retVal;
		}

		/** {@inheritDoc} */
		@Override
		public void zAdd(String key, Object data, double score) {
			int tries = 0;
			boolean sucess = false;
			do {
				tries++;
				try {
					cluster.zadd(key, score, SerializationUtil.serialize(data));
					sucess = true;
				} catch (JedisClusterMaxRedirectionsException | JedisConnectionException ex) {
					log.error(RedisConstants.CONN_FAILED_RETRY_MSG + tries);
					if (tries == numRetries) {
						throw ex;
					}
					waitforFailover();
				}
			} while (!sucess && tries <= numRetries);
		}

		/** {@inheritDoc} */
		@Override
		public Boolean zExists(String key, Object data) {
			int tries = 0;
			boolean sucess = false;
			boolean retVal = false;
			do {
				tries++;
				try {
					retVal = (cluster.zscore(key, SerializationUtil.serialize(data)) != null);
					sucess = true;
				} catch (JedisClusterMaxRedirectionsException | JedisConnectionException ex) {
					log.error(RedisConstants.CONN_FAILED_RETRY_MSG + tries);
					if (tries == numRetries) {
						throw ex;
					}
					waitforFailover();
				}
			} while (!sucess && tries <= numRetries);
			return retVal;
		}

		/** {@inheritDoc} */
		@Override
		public Long zRemove(String key, Object... data) {
			int tries = 0;
			Long retVal = null;
			boolean sucess = false;
			do {
				tries++;
				try {
					retVal = cluster.zrem(key, SerializationUtil.serialize(data));
					sucess = true;
				} catch (JedisClusterMaxRedirectionsException | JedisConnectionException ex) {
					log.error(RedisConstants.CONN_FAILED_RETRY_MSG + tries);
					if (tries == numRetries) {
						throw ex;
					}
					waitforFailover();
				}
			} while (!sucess && tries <= numRetries);
			return retVal;
		}

		/** {@inheritDoc} */
		@Override
		public Set<Object> zGet(String key, double minScore, double maxScore, int offset, int count) {
			int tries = 0;
			boolean sucess = false;
			Set<String> retVal = null;
			do {
				tries++;
				try {
					retVal = cluster.zrangeByScore(key, minScore, maxScore, offset, count);
					sucess = true;
				} catch (JedisClusterMaxRedirectionsException | JedisConnectionException ex) {
					log.error(RedisConstants.CONN_FAILED_RETRY_MSG + tries);
					if (tries == numRetries) {
						throw ex;
					}
					waitforFailover();
				}
			} while (!sucess && tries <= numRetries);
			return SerializationUtil.deSerialize(retVal);
		}

		/** {@inheritDoc} */
		@Override
		public Double zScore(String key, Object data) {
			int tries = 0;
			boolean sucess = false;
			Double retVal = null;
			do {
				tries++;
				try {
					retVal = cluster.zscore(key, SerializationUtil.serialize(data));
					sucess = true;
				} catch (JedisClusterMaxRedirectionsException | JedisConnectionException ex) {
					log.error(RedisConstants.CONN_FAILED_RETRY_MSG + tries);
					if (tries == numRetries) {
						throw ex;
					}
					waitforFailover();
				}
			} while (!sucess && tries <= numRetries);
			return retVal;
		}

		/** {@inheritDoc} */
		@Override
		public Long zCount(String key) {
			int tries = 0;
			boolean sucess = false;
			Long retVal = null;
			do {
				tries++;
				try {
					retVal = cluster.zcount(key, 0, Double.MAX_VALUE);
					sucess = true;
				} catch (JedisClusterMaxRedirectionsException | JedisConnectionException ex) {
					log.error(RedisConstants.CONN_FAILED_RETRY_MSG + tries);
					if (tries == numRetries) {
						throw ex;
					}
					waitforFailover();
				}
			} while (!sucess && tries <= numRetries);
			return retVal;
		}

		/**
		 * method to wait for handling redis fail-over
		 */
		private void waitforFailover() {
			try {
				Thread.sleep(4000);
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		}
	}

	/**
	 * Distributed Task scheduler implementation.
	 * 
	 * Serialization util implementation.
	 * 
	 * @author Ranjith Manickam
	 * @since 1.0
	 */
	private static class SerializationUtil {

		/**
		 * method to serialize the data
		 * 
		 * @param data
		 * @return
		 */
		public static String serialize(Object data) {
			return SerializationUtils.serialize((Serializable) data).toString();
		}

		/**
		 * method to serialize the data
		 * 
		 * @param data
		 * @return
		 */
		public static String[] serialize(Object... data) {
			if (data != null) {
				String[] serializedData = new String[data.length];
				for (int i = 0; i < data.length; i++) {
					serializedData[i] = serialize(data[i]);
				}
				return serializedData;
			}
			return null;
		}

		/**
		 * method to de-serialize the data
		 * 
		 * @param data
		 * @return
		 */
		public static Set<Object> deSerialize(Set<String> data) {
			if (data != null) {
				Set<Object> deSerializedData = new HashSet<>();
				for (String object : data) {
					deSerializedData.add(SerializationUtils.deserialize(object.getBytes()));
				}
				return deSerializedData;
			}
			return null;
		}
	}
}