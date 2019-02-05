package org.monitoring.oozie.zookeeper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.oozie.util.XLog;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.fusesource.hawtbuf.ByteArrayInputStream;
import org.monitoring.oozie.kafka.event.MonitoringEvent;

public class FlowRouter {
	private static final XLog LOGGER = XLog.getLog(FlowRouter.class);
	private static final int TIME_OUT = 5 * 1000;
	private static final String ENTITY = "entity";
	private static final String TRIGRAM = "trigram";
	private static final String EMPTY_REPLACE = "_";
	private static final String PATH_TEMPLATE = "/project/$" + ENTITY + "$/apps/$" + TRIGRAM + "$/monitoring";
	private static final String TOPIC_KEY = "topic";
	private ZooKeeper zookeeper;
	
	private String zookeeperServer;
	
	public FlowRouter(String zookeeperServer) {
		this.zookeeperServer = zookeeperServer;
		if(this.zookeeperServer != null) {
			initZooKeeper();
		}
	}
	
	private void initZooKeeper() {
		LOGGER.info("Init zookeeper client");
		zookeeper = Optional.ofNullable(zookeeper)
			.orElseGet(() -> {
				final CountDownLatch connectionLatch = new CountDownLatch(1);
				try {
					ZooKeeper zk = new ZooKeeper(zookeeperServer, TIME_OUT, new Watcher() {
						@Override
						public void process(WatchedEvent event) {
							if(event.getState() == KeeperState.SyncConnected) {
								connectionLatch.countDown();
							}
						}
					});
					connectionLatch.await();
					return zk;
				} catch (IOException | InterruptedException e) {
					LOGGER.error(e);
					return null;
				}
			});
		LOGGER.info("Zookeeper client Created");
	}
	
	public Optional<Properties> getEventConfiguration(MonitoringEvent event) {
		String path = getConfigurationPath(event);
		return Optional.ofNullable(zookeeper)
			.flatMap(zk -> getDataFromZk(zk, path))
			.map(ByteArrayInputStream::new)
			.flatMap(this::loadProperties);
	}
	
	private Optional<Properties> loadProperties(InputStream stream){
		try {
			Properties props = new Properties();
			props.load(stream);
			return Optional.of(props);
		} catch (IOException e) {
			LOGGER.error(e);
			return Optional.empty();
		}
	}
	
	private Optional<byte[]> getDataFromZk(ZooKeeper zk, String path){
		try {
			return Optional.of(zk.getData(path, null, null));
		} catch (KeeperException | InterruptedException e) {
			LOGGER.error(e);
			return Optional.empty();
		}
	}
	
	public Optional<String> getEventTopic(MonitoringEvent event) {
		return getEventConfiguration(event)
				.map(props -> props.getProperty(TOPIC_KEY));
	}

	private String getConfigurationPath(MonitoringEvent event) {
		StringTemplate pathTemplate = new StringTemplate(PATH_TEMPLATE);
		pathTemplate.setAttribute(ENTITY, Optional.ofNullable(event.getEntity()).orElse(EMPTY_REPLACE));
		pathTemplate.setAttribute(TRIGRAM, Optional.ofNullable(event.getTrigram()).orElse(EMPTY_REPLACE));
		String path = pathTemplate.toString();
		return path;
	}
}
