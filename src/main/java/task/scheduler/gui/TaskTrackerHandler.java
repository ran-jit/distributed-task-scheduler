package task.scheduler.gui;

import java.io.IOException;
import java.util.Set;

import javax.annotation.PreDestroy;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;

import task.scheduler.constants.TaskSchedulerConstants;
import task.scheduler.data.cache.DataCache;
import task.scheduler.data.cache.impl.RedisDataCache;

/**
 * Distributed Task scheduler implementation.
 * 
 * Task tracker gui handler
 * 
 * @author Ranjith Manickam
 * @since 1.0
 */
public class TaskTrackerHandler {

	private TaskTrackerThread taskTracker;

	/**
	 * method to start the gui server
	 * 
	 * @param port
	 */
	public void startServer(int port) {
		taskTracker = new TaskTrackerThread(port);
		Thread thread = new Thread(taskTracker);
		thread.start();
	}

	/** {@inheritDoc} */
	@PreDestroy
	public void destroy() {
		try {
			if (taskTracker != null) {
				((TaskTrackerThread) taskTracker).destroy();
			}
		} catch (Exception ex) {
			// suppress
		}
	}

	/**
	 * Distributed Task scheduler implementation.
	 * 
	 * Task tracker thread implementation.
	 * 
	 * @author Ranjith Manickam
	 * @since 1.0
	 */
	private static class TaskTrackerThread implements Runnable {

		private Server server;
		private int serverPort;

		public TaskTrackerThread(int serverPort) {
			this.serverPort = (serverPort < 0) ? TaskSchedulerConstants.DEFAULT_SCHEDULER_GUI_PORT : serverPort;
		}

		/** {@inheritDoc} */
		@Override
		public void run() {
			try {
				this.server = new Server(this.serverPort);

				ServletHandler servletHandler = new ServletHandler();
				servletHandler.addServletWithMapping(TaskTrackerServlet.class, "/tracker");
				this.server.setHandler(servletHandler);

				this.server.start();
				this.server.join();
			} catch (Exception ex) {
				// suppress
			}
		}

		/**
		 * method to destroy the server resources
		 */
		public void destroy() {
			try {
				if (server != null) {
					server.stop();
				}
			} catch (Exception ex) {
				// suppress
			}
		}
	}

	/**
	 * Distributed Task scheduler implementation.
	 * 
	 * Task tracker GUI servlet.
	 * 
	 * @author Ranjith Manickam
	 * @since 1.0
	 */
	public static class TaskTrackerServlet extends HttpServlet {

		private static final long serialVersionUID = 8640199449008147632L;

		/** {@inheritDoc} */
		protected void doGet(HttpServletRequest request, HttpServletResponse response)
				throws ServletException, IOException {

			String responseData = null;
			String action = request.getParameter("action");
			String schedulerName = request.getParameter("schedulerName");

			if (StringUtils.isNotBlank(action)) {
				switch (action) {
				case TaskSchedulerConstants.RETRY_FAILED_TASKS:
					responseData = retryFailedTasks(schedulerName);
					break;
				case TaskSchedulerConstants.DELETE_FAILED_TASKS:
					responseData = clearFailedTasks(schedulerName);
					break;
				case TaskSchedulerConstants.DELETE_TASK_SCHEDULER:
					responseData = deleteTaskScheduler(schedulerName);
					break;
				}
			} else {
				responseData = loadTaskDetailsPage();
			}

			sendResponse(response, responseData);
		}

		/**
		 * method to send response
		 * 
		 * @param response
		 * @param responseData
		 * @throws IOException
		 */
		private void sendResponse(HttpServletResponse response, String responseData) throws IOException {
			response.setContentType("text/html");
			response.setStatus(HttpServletResponse.SC_OK);
			response.getWriter().println(responseData);
		}

		/**
		 * method to get task details page informations
		 * 
		 * @return
		 */
		private String loadTaskDetailsPage() {
			StringBuffer xml = new StringBuffer();

			xml.append("<!DOCTYPE html>");
			xml.append("<html>");

			xml.append("<head>");
			xml.append("<meta charset='UTF-8'>");
			xml.append("<title>Distributed Task Scheduler</title>");

			xml.append("<style>");
			xml.append("body{background-color: #91ced4;}");
			xml.append("body *{box-sizing: border-box;}");
			xml.append(".header{padding: 1rem;text-align: center;border-radius: 5px; width: 100%}");
			xml.append(".table{width: 90%;border: 1px solid #327a81;border-radius: 10px;box-shadow: 3px 3px 0 rgba(0, 0, 0, 0.1);max-width: calc(100% - 2em);margin: 1em auto;overflow: hidden;}");
			xml.append(".table td, table th{color: #2b686e;padding: 10px;}");
			xml.append(".table td{text-align: center;vertical-align: middle;}");
			xml.append(".table th{background-color: #daeff1;font-weight: bold;}");
			xml.append(".table tr:nth-child(2n){background-color: white;}");
			xml.append(".table tr:nth-child(2n+1){background-color: #edf7f8;}");
			xml.append("</style>");

			xml.append("<script>");
			xml.append("function retryFailedTasks(jobKey) {");
			xml.append("var isProcess = confirm('Are you sure want to retry failed tasks..');");
			xml.append("if(isProcess) {");
			xml.append("}}");
			xml.append("function clearFailedTasks(jobKey) {");
			xml.append("var isProcess = confirm('Are you sure want to clear failed tasks..');");
			xml.append("if(isProcess) {");
			xml.append("}}");
			xml.append("function deleteTaskScheduler(jobKey) {");
			xml.append("var isProcess = confirm('Are you sure want to delete task scheduler and data..');");
			xml.append("if(isProcess) {");
			xml.append("}}");
			xml.append("</script>");

			xml.append("</head>");

			xml.append("<body>");
			xml.append("<div>");
			xml.append("<table cellspacing='0' class='header'>");
			xml.append("<tr>");
			xml.append("<td>");
			xml.append("<a href='http://www.ranmanic.in' target='_blank'><img src='http://www.ranmanic.in/assets/images/logo/logo-100x100.png' alt='Ranjith Manickam'></a>");
			xml.append("</td>");
			xml.append("<td>");
			xml.append("<span style='font-weight:bold;font-size: 1.5em;'><a href='https://github.com/ran-jit/distributed-task-scheduler' target='_blank' style='text-decoration: none !important;'>Distributed Task Scheduler</a></span>");
			xml.append("<br><br>Distributed task scheduler is a Java implementation for handling tasks in distributed environment using Redis. (<a href='https://github.com/ran-jit/distributed-task-scheduler' target='_blank'>Source</a>)");
			xml.append("</td>");
			xml.append("</tr>");
			xml.append("</table>");

			DataCache dataCache = null;
			Set<String> schedulerInfo = null;
			try {
				dataCache = new RedisDataCache();
				schedulerInfo = dataCache.sMembers(TaskSchedulerConstants.TASK_SCHEDULER_INFO);
			} catch (Exception ex) {
				// suppress
			}

			if (schedulerInfo == null || schedulerInfo.isEmpty()) {
				xml.append("<div style='width:100%; text-align:center;font-weight: bold;'>");
				xml.append("<br><br><br>");
				xml.append("No Task schedulers exists..");
				xml.append("</div>");
			} else {
				xml.append("<table cellspacing='0' class='table' style='width:90%'>");
				xml.append("<tr>");
				xml.append("<th width=40%>Name</th>");
				xml.append("<th>Pending Tasks</th>");
				xml.append("<th>Failed Tasks</th>");
				xml.append("<th>Redis Key</th>");
				xml.append("<th>Retry Failed Tasks</th>");
				xml.append("<th>Clear Failed Tasks</th>");
				xml.append("<th>Delete Scheduler</th>");
				xml.append("</tr>");

				for (String schedulerName : schedulerInfo) {
					xml.append("<tr>");
					xml.append("<td style='text-align: left; padding-left: 20px;'>");
					xml.append(schedulerName);
					xml.append("</td>");
					xml.append("<td>");
					xml.append(dataCache.zCount(schedulerName));
					xml.append("</td>");
					xml.append("<td>");
					xml.append(dataCache.zCount(schedulerName.concat(TaskSchedulerConstants.TASK_SCHEDULER_DLQ_EXTN)));
					xml.append("</td>");
					xml.append("<td>");
					xml.append(RedisDataCache.parseDataCacheKey(schedulerName));
					xml.append("</td>");
					xml.append("<td><a href='#' onclick=\"retryFailedTasks('");
					xml.append(schedulerName);
					xml.append("')\">click here</a></td>");
					xml.append("<td><a href='#' onclick=\"clearFailedTasks('");
					xml.append(schedulerName);
					xml.append("')\">click here</a></td>");
					xml.append("<td><a href='#' onclick=\"deleteTaskScheduler('");
					xml.append(schedulerName);
					xml.append("')\">click here</a></td>");
					xml.append("</tr>");
				}
				xml.append("</table>");
			}

			xml.append("</div>");
			xml.append("</body>");

			xml.append("</html>");

			return xml.toString();
		}

		/**
		 * method to retry failed tasks
		 * 
		 * @param schedulerName
		 * @return
		 */
		private String retryFailedTasks(String schedulerName) {
			// working on it..
			return null;
		}

		/**
		 * method to clear failed tasks
		 * 
		 * @param schedulerName
		 * @return
		 */
		private String clearFailedTasks(String schedulerName) {
			// working on it..
			return null;
		}

		/**
		 * method to delete task scheduler
		 * 
		 * @param schedulerName
		 * @return
		 */
		private String deleteTaskScheduler(String schedulerName) {
			// working on it..
			return null;
		}
	}
}