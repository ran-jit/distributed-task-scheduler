package task.scheduler.test;

import java.util.Date;

import task.scheduler.TaskScheduler;
import task.scheduler.impl.TaskSchedulerImpl;

/**
 * Distributed Task scheduler implementation.
 * 
 * Test :: Task scheduler implementation.
 * 
 * @author Ranjith Manickam
 * @since 1.0
 */
public class TaskSchedulerImplTest {

	public static void main(String[] args) {
		start();
	}

	public static void start() {

		// !-------------------------------------------------------------------------------------
		// scheduler instance
		TaskScheduler scheduler1 = new TaskSchedulerImpl("Scheduler 1", TaskTriggerListenerImplTest.class);
		// start the scheduler
		scheduler1.start();

		// scheduling the task
		scheduler1.schedule("test-data1", new Date());

		// !-------------------------------------------------------------------------------------

		int pollingTreadSize = 3;

		// scheduler instance
		TaskScheduler scheduler2 = new TaskSchedulerImpl("Scheduler-2", pollingTreadSize, TaskTriggerListenerImplTest.class);
		// start the scheduler
		scheduler2.start();

		// scheduling the task
		scheduler2.schedule("test-data2", new Date());
	}

}