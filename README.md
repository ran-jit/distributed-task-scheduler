# Distributed Task Scheduler
Distributed Scheduler using Redis data cache.

Distributed task scheduler is a Java implementation for handling tasks in distributed environment using Redis. It has the following features:

1. Useable in a distributed environment: It uses Redis data cache for effectively preventing a task to be run on multiple instances of the same application.
2. Configurable consumers: Polling consumers can be configured per queue.
3. Configurable polling: Polling delay can be configured to tweak execution precision.
4. Multiple schedulers support: You can create multiple schedulers in the same application.


## Interfaces:

### TaskScheduler
This is the interface where you submit your tasks for future execution. Once the tasks are submitted, based on the trigger time tasks will be executed. (Implementation: TaskSchedulerImpl)

### TaskTriggerListener
This is the interface you must implement to actually run the tasks once they are due for execution. This library will call the "execute" method for each task when the task is ready for execution.

### Note:
Examples are committed in test package.
