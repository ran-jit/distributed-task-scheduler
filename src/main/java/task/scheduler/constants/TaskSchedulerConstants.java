package task.scheduler.constants;

/**
 * Distributed Task scheduler implementation.
 * 
 * Task scheduler constants.
 * 
 * @author Ranjith Manickam
 * @since 1.0
 */
public class TaskSchedulerConstants {

	// task scheduler properties file name
	public static final String PROPERTIES_FILE = "task-scheduler.properties";

	// default values
	public static final int DEFAULT_POLL_DATA_DELAY = 2;
	public static final int DEFAULT_POLLING_THREAD_SIZE = 1;
	public static final int DEFAULT_PROCESS_DATA_COUNT = 10;

	public static final String TASK_SCHEDULER_DLQ_EXTN = "_dlq";

	public static final String TASK_EXEC_FAILED_RETRY_MSG = "Task execution failed, retrying...";
}