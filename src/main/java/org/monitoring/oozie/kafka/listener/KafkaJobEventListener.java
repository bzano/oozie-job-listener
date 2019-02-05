package org.monitoring.oozie.kafka.listener;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.event.JobEvent;
import org.apache.oozie.event.BundleJobEvent;
import org.apache.oozie.event.CoordinatorActionEvent;
import org.apache.oozie.event.CoordinatorJobEvent;
import org.apache.oozie.event.WorkflowActionEvent;
import org.apache.oozie.event.WorkflowJobEvent;
import org.apache.oozie.event.listener.JobEventListener;
import org.apache.oozie.util.XLog;
import org.monitoring.oozie.kafka.event.MonitoringEvent;
import org.monitoring.oozie.kafka.producer.KafkaEventProducer;

public class KafkaJobEventListener extends JobEventListener {
	private static final XLog LOGGER = XLog.getLog(KafkaJobEventListener.class);
	private static final String INFO = "INFO";

	private KafkaEventProducer kafkaProducer;
	
	@Override
	public void init(Configuration conf) {
		String kafkaServer = conf.get("oozie.job.listener.kafka.bootstrap.servers");
		String zookeeperServer = conf.get("oozie.job.listener.zookeeper");
		LOGGER.info("init kafka job listener (" + kafkaServer + " / " + zookeeperServer + ")");
		kafkaProducer = new KafkaEventProducer(kafkaServer, zookeeperServer);
	}

	@Override
	public void destroy() {
		LOGGER.info("destroy kafka");
	}

	@Override
	public void onWorkflowJobEvent(WorkflowJobEvent wje) {
		LOGGER.info("onWorkflowJobEvent kafka");
		MonitoringEvent event = jobEventToMonitoringEvent(wje);
		kafkaProducer.sendEvent(event);
	}

	private MonitoringEvent jobEventToMonitoringEvent(JobEvent jobEvent) {
		MonitoringEvent event = new MonitoringEvent();
		event.setJobName(jobEvent.getAppName());
		event.setSeverity(INFO);
		event.setTsStart(jobEvent.getStartTime().getTime());
		event.setTsEnd(jobEvent.getEndTime().getTime());
		event.setTsDuration(event.getTsEnd() - event.getTsStart());
		event.setUser(jobEvent.getUser());
		event.setJobType(jobEvent.getAppType().name());
		event.setStatus(jobEvent.getEventStatus().name());
		return event;
	}

	@Override
	public void onWorkflowActionEvent(WorkflowActionEvent wae) {
		LOGGER.info("onWorkflowActionEvent kafka");
		MonitoringEvent event = jobEventToMonitoringEvent(wae);
		kafkaProducer.sendEvent(event);
	}

	@Override
	public void onCoordinatorJobEvent(CoordinatorJobEvent cje) {
		LOGGER.info("onCoordinatorJobEvent kafka");
		MonitoringEvent event = jobEventToMonitoringEvent(cje);
		kafkaProducer.sendEvent(event);
	}

	@Override
	public void onCoordinatorActionEvent(CoordinatorActionEvent cae) {
		LOGGER.info("onCoordinatorActionEvent kafka");	
		MonitoringEvent event = jobEventToMonitoringEvent(cae);
		kafkaProducer.sendEvent(event);
	}

	@Override
	public void onBundleJobEvent(BundleJobEvent bje) {
		LOGGER.info("onBundleJobEvent kafka");	
		MonitoringEvent event = jobEventToMonitoringEvent(bje);
		kafkaProducer.sendEvent(event);
	}
}
