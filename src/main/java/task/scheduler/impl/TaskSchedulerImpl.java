package task.scheduler.impl;

import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PreDestroy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import task.scheduler.TaskScheduler;
import task.scheduler.constants.TaskSchedulerConstants;
import task.scheduler.data.cache.DataCache;
import task.scheduler.data.cache.impl.RedisDataCache;
import task.scheduler.trigger.listener.TaskTriggerListener;

/**
 * Distributed Task scheduler implementation.
 * 
 * Task scheduler implementation for scheduling the data objects with trigger time.
 * 
 * @author Ranjith Manickam
 * @since 1.0
 */
public class TaskSchedulerImpl implements TaskScheduler {

	private Log log = LogFactory.getLog(TaskSchedulerImpl.class);

	private String schedulerName;
	private int pollingTreadSize;
	private int pollingDelayMillis;
	private int processDataCount;

	private DataCache dataCache;
	private Class<?> taskTriggerListener;

	private ExecutorService threadPool;
	private List<Runnable> pollingThreads;

	/**
	 * Task scheduler implementation
	 * 
	 * @param schedulerName - scheduler name
	 * @param taskTriggerListener - task action listener class
	 */
	public TaskSchedulerImpl(String schedulerName, Class<?> taskTriggerListener) {
		this(schedulerName, TaskSchedulerConstants.DEFAULT_POLLING_THREAD_SIZE, taskTriggerListener);
	}

	/**
	 * Task scheduler implementation
	 * 
	 * @param schedulerName - scheduler name
	 * @param taskSchedulerProperties - scheduler properties
	 * @param taskTriggerListener - task action listener class
	 */
	public TaskSchedulerImpl(String schedulerName, Properties taskSchedulerProperties, Class<?> taskTriggerListener) {
		this(schedulerName, TaskSchedulerConstants.DEFAULT_POLLING_THREAD_SIZE, taskSchedulerProperties, taskTriggerListener);
	}

	/**
	 * Task scheduler implementation
	 * 
	 * @param schedulerName - scheduler name
	 * @param taskSchedulerPropertiesFilePath - scheduler properties file path
	 * @param taskTriggerListener - task action listener class
	 */
	public TaskSchedulerImpl(String schedulerName, String taskSchedulerPropertiesFilePath,
			Class<?> taskTriggerListener) {
		this(schedulerName, TaskSchedulerConstants.DEFAULT_POLLING_THREAD_SIZE, taskSchedulerPropertiesFilePath, taskTriggerListener);
	}

	/**
	 * Task scheduler implementation
	 * 
	 * @param schedulerName - scheduler name
	 * @param pollingTreadSize - polling thread size
	 * @param taskTriggerListener - task action listener class
	 */
	public TaskSchedulerImpl(String schedulerName, int pollingTreadSize, Class<?> taskTriggerListener) {
		this(schedulerName, pollingTreadSize, TaskSchedulerConstants.DEFAULT_POLL_DATA_DELAY, taskTriggerListener);
	}

	/**
	 * Task scheduler implementation
	 * 
	 * @param schedulerName - scheduler name
	 * @param pollingTreadSize - polling thread size
	 * @param taskSchedulerProperties - scheduler properties
	 * @param taskTriggerListener - task action listener class
	 */
	public TaskSchedulerImpl(String schedulerName, int pollingTreadSize, Properties taskSchedulerProperties,
			Class<?> taskTriggerListener) {
		this(schedulerName, pollingTreadSize, TaskSchedulerConstants.DEFAULT_POLL_DATA_DELAY, taskSchedulerProperties, taskTriggerListener);
	}

	/**
	 * Task scheduler implementation
	 * 
	 * @param schedulerName - scheduler name
	 * @param pollingTreadSize - polling thread size
	 * @param taskSchedulerProperties - scheduler properties file path
	 * @param taskTriggerListener - task action listener class
	 */
	public TaskSchedulerImpl(String schedulerName, int pollingTreadSize, String taskSchedulerPropertiesFilePath,
			Class<?> taskTriggerListener) {
		this(schedulerName, pollingTreadSize, TaskSchedulerConstants.DEFAULT_POLL_DATA_DELAY,
				taskSchedulerPropertiesFilePath, taskTriggerListener);
	}

	/**
	 * Task scheduler implementation
	 * 
	 * @param schedulerName - scheduler name
	 * @param pollingTreadSize - polling thread size
	 * @param pollingDelayMillis - polling delay in milliseconds
	 * @param taskTriggerListener - task action listener class
	 */
	public TaskSchedulerImpl(String schedulerName, int pollingTreadSize, int pollingDelayMillis,
			Class<?> taskTriggerListener) {
		this(schedulerName, pollingTreadSize, pollingDelayMillis, TaskSchedulerConstants.DEFAULT_PROCESS_DATA_COUNT, taskTriggerListener);
	}

	/**
	 * Task scheduler implementation
	 * 
	 * @param schedulerName - scheduler name
	 * @param pollingTreadSize - polling thread size
	 * @param pollingDelayMillis - polling delay in milliseconds
	 * @param taskSchedulerProperties - scheduler properties
	 * @param taskTriggerListener - task action listener class
	 */
	public TaskSchedulerImpl(String schedulerName, int pollingTreadSize, int pollingDelayMillis,
			Properties taskSchedulerProperties, Class<?> taskTriggerListener) {
		this(schedulerName, pollingTreadSize, pollingDelayMillis, TaskSchedulerConstants.DEFAULT_PROCESS_DATA_COUNT,
				taskSchedulerProperties, taskTriggerListener);
	}

	/**
	 * Task scheduler implementation
	 * 
	 * @param schedulerName - scheduler name
	 * @param pollingTreadSize - polling thread size
	 * @param pollingDelayMillis - polling delay in milliseconds
	 * @param taskSchedulerPropertiesFilePath - scheduler properties file path
	 * @param taskTriggerListener - task action listener class
	 */
	public TaskSchedulerImpl(String schedulerName, int pollingTreadSize, int pollingDelayMillis,
			String taskSchedulerPropertiesFilePath, Class<?> taskTriggerListener) {
		this(schedulerName, pollingTreadSize, pollingDelayMillis, TaskSchedulerConstants.DEFAULT_PROCESS_DATA_COUNT,
				taskSchedulerPropertiesFilePath, taskTriggerListener);
	}

	/**
	 * Task scheduler implementation
	 * 
	 * @param schedulerName - scheduler name
	 * @param pollingTreadSize - polling thread size
	 * @param pollingDelayMillis - polling delay in milliseconds
	 * @param processDataCount - process data count
	 * @param taskTriggerListener - task action listener class
	 */
	public TaskSchedulerImpl(String schedulerName, int pollingTreadSize, int pollingDelayMillis, int processDataCount,
			Class<?> taskTriggerListener) {
		this.schedulerName = schedulerName;
		this.pollingTreadSize = pollingTreadSize;
		this.processDataCount = processDataCount;
		this.pollingDelayMillis = pollingDelayMillis;
		this.taskTriggerListener = taskTriggerListener;
		this.dataCache = new RedisDataCache();
	}

	/**
	 * Task scheduler implementation
	 * 
	 * @param schedulerName - scheduler name
	 * @param pollingTreadSize - polling thread size
	 * @param pollingDelayMillis - polling delay in milliseconds
	 * @param processDataCount - process data count
	 * @param taskSchedulerProperties - scheduler properties
	 * @param taskTriggerListener - task action listener class
	 */
	public TaskSchedulerImpl(String schedulerName, int pollingTreadSize, int pollingDelayMillis, int processDataCount,
			Properties taskSchedulerProperties, Class<?> taskTriggerListener) {
		this.schedulerName = schedulerName;
		this.pollingTreadSize = pollingTreadSize;
		this.processDataCount = processDataCount;
		this.pollingDelayMillis = pollingDelayMillis;
		this.taskTriggerListener = taskTriggerListener;
		this.dataCache = new RedisDataCache(taskSchedulerProperties);
	}

	/**
	 * Task scheduler implementation
	 * 
	 * @param schedulerName - scheduler name
	 * @param pollingTreadSize - polling thread size
	 * @param pollingDelayMillis - polling delay in milliseconds
	 * @param processDataCount - process data count
	 * @param taskSchedulerPropertiesFilePath - scheduler properties file path
	 * @param taskTriggerListener - task action listener class
	 */
	public TaskSchedulerImpl(String schedulerName, int pollingTreadSize, int pollingDelayMillis, int processDataCount,
			String taskSchedulerPropertiesFilePath, Class<?> taskTriggerListener) {
		this.schedulerName = schedulerName;
		this.pollingTreadSize = pollingTreadSize;
		this.processDataCount = processDataCount;
		this.pollingDelayMillis = pollingDelayMillis;
		this.taskTriggerListener = taskTriggerListener;
		this.dataCache = new RedisDataCache(taskSchedulerPropertiesFilePath);
	}

	/** {@inheritDoc} */
	@Override
	public void start() {
		try {
			this.threadPool = Executors.newFixedThreadPool(this.pollingTreadSize);
			for (int i = 0; i < this.pollingTreadSize; i++) {
				// example: pollingTreadSize = 3 and processDataCount = 10
				// thread-1 poll data from 0->10
				// thread-2 poll data from 11->20
				// thread-3 poll data from 21->30
				int processDataOffset = (i * this.processDataCount) + this.processDataCount;

				Runnable worker = new TaskSchedulerThreadImpl(this.dataCache, this.taskTriggerListener,
						this.schedulerName, processDataOffset, this.processDataCount, this.pollingDelayMillis,
						this.pollingTreadSize);

				this.pollingThreads.add(worker);
				this.threadPool.execute(worker);
			}
		} catch (Exception ex) {
			log.error("Error while starting Task scheduler", ex);
		}
	}

	/** {@inheritDoc} */
	@Override
	public void stop() {
		destroy();
	}

	/** {@inheritDoc} */
	@Override
	public void schedule(Object data, Date triggerTime) {
		this.dataCache.add(this.schedulerName, data, triggerTime.getTime());
	}

	/** {@inheritDoc} */
	@PreDestroy
	public void destroy() {
		if (this.pollingThreads != null && !this.pollingThreads.isEmpty()) {
			for (Runnable pollingThread : this.pollingThreads) {
				((TaskSchedulerThreadImpl) pollingThread).stop();
			}
		}
		if (this.threadPool != null && !this.threadPool.isTerminated()) {
			this.threadPool.shutdown();
		}
	}

	/**
	 * Distributed Task scheduler implementation.
	 * 
	 * Task scheduler thread for processing scheduled data batch.
	 * 
	 * @author Ranjith Manickam
	 * @since 1.0
	 */
	private class TaskSchedulerThreadImpl implements Runnable {

		private String schedulerName;
		private int pollingTreadSize;
		private int processDataCount;
		private int processDataOffset;
		private int pollingDelayMillis;

		private DataCache dataCache;
		private Class<?> taskTriggerListener;

		private boolean isAlive = true;
		private ExecutorService threadPool;
		private List<Runnable> pollingThreads;

		public TaskSchedulerThreadImpl(DataCache dataCache, Class<?> taskTriggerListener, String schedulerName,
				int processDataOffset, int processDataCount, int pollingDelayMillis, int pollingTreadSize) {

			this.schedulerName = schedulerName;
			this.processDataCount = processDataCount;
			this.processDataOffset = processDataOffset;
			this.pollingDelayMillis = pollingDelayMillis;

			this.dataCache = dataCache;
			this.taskTriggerListener = taskTriggerListener;
			this.threadPool = Executors.newFixedThreadPool(this.pollingTreadSize);
		}

		/** {@inheritDoc} */
		@Override
		public void run() {
			while (this.isAlive) {
				boolean continueExec = triggerTasks();
				if (!continueExec) {
					try {
						Thread.sleep(this.pollingDelayMillis);
					} catch (Exception ex) {
						// suppress
					}
				}
			}
		}

		/**
		 * method to stop the scheduler thread execution
		 */
		public void stop() {
			this.isAlive = false;

			if (this.pollingThreads != null && !this.pollingThreads.isEmpty()) {
				for (Runnable pollingThread : this.pollingThreads) {
					((TaskTriggerListenerThreadImpl) pollingThread).stop();
				}
			}
			if (this.threadPool != null && !this.threadPool.isTerminated()) {
				this.threadPool.shutdown();
			}
		}

		/**
		 * method to trigger scheduled tasks
		 * 
		 * @return
		 */
		private boolean triggerTasks() {
			Set<Object> tasks = this.dataCache.get(this.schedulerName, 0, System.currentTimeMillis(),
					this.processDataOffset, this.processDataCount);

			if (tasks != null && !tasks.isEmpty()) {
				for (Object task : tasks) {
					Runnable worker = new TaskTriggerListenerThreadImpl(this.dataCache, this.taskTriggerListener,
							this.schedulerName, task);

					this.pollingThreads.add(worker);
					this.threadPool.execute(worker);
				}
				return true;
			}
			return false;
		}
	}

	/**
	 * Distributed Task scheduler implementation.
	 * 
	 * Task trigger listener thread for processing scheduled data.
	 * 
	 * @author Ranjith Manickam
	 * @since 1.0
	 */
	private class TaskTriggerListenerThreadImpl implements Runnable {

		private Object task;
		private DataCache dataCache;
		private String schedulerName;
		private TaskTriggerListener taskTriggerListener;

		private final int numRetries = 3;
		private boolean isAlive = true;

		public TaskTriggerListenerThreadImpl(DataCache dataCache, Class<?> taskTriggerListener, String schedulerName,
				Object task) {
			this.task = task;
			this.dataCache = dataCache;
			this.schedulerName = schedulerName;
			try {
				this.taskTriggerListener = (TaskTriggerListener) taskTriggerListener.getConstructor().newInstance();
			} catch (Exception ex) {
				log.error("Error while creating 'TaskTriggerListener' instance", ex);
			}
		}

		/** {@inheritDoc} */
		@Override
		public void run() {
			int tries = 0;
			boolean sucess = false;
			do {
				if (!this.isAlive) {
					break;
				}

				tries++;
				try {
					if (this.dataCache.exists(this.schedulerName, this.task)) {
						this.taskTriggerListener.execute(this.task);
						this.dataCache.remove(this.schedulerName, this.task);
					}
					sucess = true;
				} catch (Exception ex) {
					log.error(TaskSchedulerConstants.TASK_EXEC_FAILED_RETRY_MSG + tries);
					if (tries == numRetries) {
						processFailedTask();
					}
				}
			} while (!sucess && tries <= numRetries);
		}

		/**
		 * method to process the failed task
		 */
		private void processFailedTask() {
			if (this.dataCache.exists(this.schedulerName, this.task)) {
				Double score = this.dataCache.getScore(this.schedulerName, this.task);
				this.dataCache.add(this.schedulerName.concat(TaskSchedulerConstants.TASK_SCHEDULER_DLQ_EXTN), this.task, score);
				this.dataCache.remove(this.schedulerName, this.task);
			}
		}

		/**
		 * method to stop the scheduler thread execution
		 */
		public void stop() {
			this.isAlive = false;

			if (this.taskTriggerListener != null) {
				this.taskTriggerListener.destroy();
			}
		}
	}

}