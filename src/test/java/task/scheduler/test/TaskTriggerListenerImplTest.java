package task.scheduler.test;

import task.scheduler.trigger.listener.TaskTriggerListener;

/**
 * Distributed Task scheduler implementation.
 * 
 * Test :: Task trigger listener implementation.
 * 
 * @author Ranjith Manickam
 * @since 1.0
 */
public class TaskTriggerListenerImplTest implements TaskTriggerListener {

	@Override
	public void execute(Object data) {
		System.out.println((String) data);
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub
	}

}