package org.monitoring.oozie.kafka.listener;

import java.util.Optional;

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
		String zkPathPrefix = conf.get("oozie.job.listener.zookeeper.path.prefix");
		LOGGER.info("init kafka job listener (" + kafkaServer + " / " + zookeeperServer + " / " + zkPathPrefix + ")");
		kafkaProducer = new KafkaEventProducer(kafkaServer, zookeeperServer, zkPathPrefix);
	}

	@Override
	public void destroy() {
		LOGGER.info("destroy kafka");
	}

	@Override
	public void onWorkflowJobEvent(WorkflowJobEvent wje) {
		LOGGER.info("onWorkflowJobEvent kafka");
		Optional<MonitoringEvent> event = jobEventToMonitoringEvent(wje);
		event.ifPresent(kafkaProducer::sendEvent);
	}

	private Optional<MonitoringEvent> jobEventToMonitoringEvent(JobEvent jobEvent) {
		if(jobEvent.getEndTime() != null) {
			MonitoringEvent event = new MonitoringEvent();
			event.setJobName(jobEvent.getAppName());
			event.setSeverity(INFO);
			event.setTsStart(jobEvent.getStartTime().getTime());
			event.setTsEnd(jobEvent.getEndTime().getTime());
			event.setTsDuration(event.getTsEnd() - event.getTsStart());
			event.setUser(jobEvent.getUser());
			event.setJobType(jobEvent.getAppType().name());
			event.setStatus(jobEvent.getEventStatus().name());
			return Optional.of(event);	
		}
		return Optional.empty();
	}

	@Override
	public void onWorkflowActionEvent(WorkflowActionEvent wae) {
		LOGGER.info("onWorkflowActionEvent kafka");
		Optional<MonitoringEvent> event = jobEventToMonitoringEvent(wae);
		event.ifPresent(kafkaProducer::sendEvent);
	}

	@Override
	public void onCoordinatorJobEvent(CoordinatorJobEvent cje) {
		LOGGER.info("onCoordinatorJobEvent kafka");
		Optional<MonitoringEvent> event = jobEventToMonitoringEvent(cje);
		event.ifPresent(kafkaProducer::sendEvent);
	}

	@Override
	public void onCoordinatorActionEvent(CoordinatorActionEvent cae) {
		LOGGER.info("onCoordinatorActionEvent kafka");	
		Optional<MonitoringEvent> event = jobEventToMonitoringEvent(cae);
		event.ifPresent(kafkaProducer::sendEvent);
	}

	@Override
	public void onBundleJobEvent(BundleJobEvent bje) {
		LOGGER.info("onBundleJobEvent kafka");	
		Optional<MonitoringEvent> event = jobEventToMonitoringEvent(bje);
		event.ifPresent(kafkaProducer::sendEvent);
	}
}
