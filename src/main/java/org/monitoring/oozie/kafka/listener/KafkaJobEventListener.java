package org.monitoring.oozie.kafka.listener;

import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.event.JobEvent;
import org.apache.oozie.client.event.JobEvent.EventStatus;
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
	private static final String ERROR = "ERROR";

	private KafkaEventProducer kafkaProducer;

	@Override
	public void init(Configuration conf) {
		String kafkaServer = conf.get("oozie.job.listener.kafka.bootstrap.servers");
		String zookeeperServer = conf.get("oozie.job.listener.zookeeper");
		String zkPathPrefix = conf.get("oozie.job.listener.zookeeper.path.prefix");
		LOGGER.info("Init kafka job listener (" + kafkaServer + " / " + zookeeperServer + " / " + zkPathPrefix + ")");
		kafkaProducer = new KafkaEventProducer(kafkaServer, zookeeperServer, zkPathPrefix);
	}

	@Override
	public void destroy() {
		LOGGER.info("destroyed");
	}

	@Override
	public void onWorkflowJobEvent(WorkflowJobEvent wje) {
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("onWorkflowJobEvent to kafka");
		}
		Optional<MonitoringEvent> event = jobEventToMonitoringEvent(wje);
		event.ifPresent(kafkaProducer::sendEvent);
	}

	private Optional<MonitoringEvent> jobEventToMonitoringEvent(JobEvent jobEvent) {
		if (jobEvent.getEndTime() != null) {
			try {
				MonitoringEvent event = new MonitoringEvent();
				event.setJobName(jobEvent.getAppName());
				String severity = getJobSeverity(jobEvent);
				event.setSeverity(severity);
				event.setTsStart(jobEvent.getStartTime().getTime());
				event.setTsEnd(jobEvent.getEndTime().getTime());
				event.setTsDuration(event.getTsEnd() - event.getTsStart());
				event.setUser(jobEvent.getUser());
				event.setJobType(jobEvent.getAppType().name());
				event.setStatus(jobEvent.getEventStatus().name());
				return Optional.of(event);
			} catch (Throwable th) {
				LOGGER.error("Cound not generate event of " + jobEvent.toString());
			}
		}
		return Optional.empty();
	}

	private String getJobSeverity(JobEvent jobEvent) {
		return Optional.of(jobEvent.getEventStatus()).filter(EventStatus.FAILURE::equals).map(st -> ERROR).orElse(INFO);
	}

	@Override
	public void onWorkflowActionEvent(WorkflowActionEvent wae) {
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("onWorkflowActionEvent to kafka");
		}
		Optional<MonitoringEvent> event = jobEventToMonitoringEvent(wae);
		event.ifPresent(kafkaProducer::sendEvent);
	}

	@Override
	public void onCoordinatorJobEvent(CoordinatorJobEvent cje) {
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("onCoordinatorJobEvent to kafka");
		}
		Optional<MonitoringEvent> event = jobEventToMonitoringEvent(cje);
		event.ifPresent(kafkaProducer::sendEvent);
	}

	@Override
	public void onCoordinatorActionEvent(CoordinatorActionEvent cae) {
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("onCoordinatorActionEvent to kafka");
		}
		Optional<MonitoringEvent> event = jobEventToMonitoringEvent(cae);
		event.ifPresent(kafkaProducer::sendEvent);
	}

	@Override
	public void onBundleJobEvent(BundleJobEvent bje) {
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("onBundleJobEvent to kafka");
		}
		Optional<MonitoringEvent> event = jobEventToMonitoringEvent(bje);
		event.ifPresent(kafkaProducer::sendEvent);
	}
}
