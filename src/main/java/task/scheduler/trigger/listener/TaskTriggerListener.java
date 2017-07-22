package task.scheduler.trigger.listener;

/**
 * Distributed Task scheduler implementation.
 * 
 * Task trigger listener interface to execute the data objects.
 * 
 * @author Ranjith Manickam
 * @since 1.0
 */
public interface TaskTriggerListener {

	/**
	 * To execute data
	 * 
	 * @param data
	 */
	void execute(Object data);

	/**
	 * destroy
	 */
	void destroy();
}