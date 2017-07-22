# Distributed Task Scheduler
Distributed Scheduler using Redis data cache.

Distributed task scheduler is a Java implementation for handling tasks in distributed environment using Redis. It has the following features:

1. Usable in a distributed environment: It uses Redis data cache for effectively preventing a task to be run on multiple instances of the same application.
2. Configurable consumers: Polling consumers can be configured per queue.
3. Configurable polling: Polling delay can be configured to tweak execution precision.
4. Multiple schedulers support: You can create multiple schedulers in the same application.

more details..
  https://github.com/ran-jit/distributed-task-scheduler/wiki


## Scheduler creation properties
	 * @param String schedulerName - Scheduler name
	 * @param Integer pollingTreadSize - Polling thread size
	 * @param Integer pollingDelayMillis - Polling delay in milliseconds
	 * @param Integer processDataCount - Process data count
	 * @param Class taskTriggerListener - Task trigger action listener class

	 * @param taskSchedulerProperties
	          1. @default: Scheduler configuration properties (by default the properties are loaded from "task-scheduler.properties" file from internal resources)
	          2.  @param String taskSchedulerPropertiesFilePath - Scheduler properties file path
	          3.  @param Properties taskSchedulerProperties - Scheduler properties

	 * refer --> "src/main/resources/task-scheduler.properties"


## Interfaces:

### TaskScheduler
This is the interface where you submit your tasks for future execution. Once the tasks are submitted, based on the trigger time tasks will be executed. (Implementation: TaskSchedulerImpl)

### TaskTriggerListener
This is the interface you must implement to actually run the tasks once they are due for execution. This library will call the "execute" method for each task when the task is ready for execution.

### Note:
Examples are committed in test package.
