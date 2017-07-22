package task.scheduler;

import java.util.Date;

/**
 * Distributed Task scheduler implementation.
 * 
 * Interface to create scheduler and schedule the data with trigger time.
 * 
 * @author Ranjith Manickam
 * @since 1.0
 */
public interface TaskScheduler {

	/**
	 * To start the scheduler
	 */
	void start();

	/**
	 * To stop the scheduler
	 */
	void stop();

	/**
	 * To schedule data with trigger time in scheduler
	 * 
	 * @param data
	 * @param triggerTime
	 */
	void schedule(Object data, Date triggerTime);

}