package blockchain;

import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

public class ValidationScheduler {
public static void main(String[] args) throws SchedulerException {
	JobDetail job = JobBuilder.newJob(ValidateBlockchains.class).withIdentity("dummyJobName", "group1").build();
	Trigger trigger = TriggerBuilder
			.newTrigger()
			.withIdentity("dummyTriggerName", "group1")
			.withSchedule(
				CronScheduleBuilder.cronSchedule("0 0 */2 * *"))
			.build();
	Scheduler scheduler = new StdSchedulerFactory().getScheduler();
	scheduler.start();
	scheduler.scheduleJob(job, trigger);
}
}
