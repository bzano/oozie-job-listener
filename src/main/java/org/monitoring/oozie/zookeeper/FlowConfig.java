package org.monitoring.oozie.zookeeper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.oozie.util.XLog;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.fusesource.hawtbuf.ByteArrayInputStream;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class FlowConfig {
	private static final XLog LOGGER = XLog.getLog(FlowConfig.class);
	private static final int TIME_OUT = 10 * 1000;
	private static final String JOB_NAME = "job_name";
	private static final Cache<String, Properties> CACHE = CacheBuilder.newBuilder()
			.expireAfterAccess(1, TimeUnit.HOURS)
			.build();
	private ZooKeeper zookeeper;
	
	private String pathTemplate;
	private String zookeeperServer;
	
	public FlowConfig(String zookeeperServer, String zkPathPrefix) {
		this.zookeeperServer = zookeeperServer;
		pathTemplate = zkPathPrefix + "/$" + JOB_NAME + "$";
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
					boolean zkInitiated = connectionLatch.await(TIME_OUT, TimeUnit.MILLISECONDS);
					if(!zkInitiated) {
						throw new IOException("Zk Init failed");
					}
					return zk;
				} catch (IOException | InterruptedException e) {
					LOGGER.error(e);
					return null;
				}
			});
		LOGGER.info("Zookeeper client Created");
	}
	
	public synchronized Optional<Properties> getEventConfiguration(String jobName) {
		Properties props = CACHE.getIfPresent(jobName);
		Optional<Properties> properties = Optional.ofNullable(Optional.ofNullable(props)
			.orElseGet(() -> {
				Optional<String> path = getConfigurationPath(jobName);
				return Optional.ofNullable(zookeeper)
					.flatMap(zk -> path.flatMap(p -> getDataFromZk(zk, p)))
					.map(ByteArrayInputStream::new)
					.flatMap(stream -> loadProperties(jobName, stream)).orElse(null);
			}));
		properties.ifPresent(p -> CACHE.put(jobName, p));
		return properties;
	}
	
	private Optional<Properties> loadProperties(String jobName, InputStream stream){
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
			LOGGER.error("Error while getting data from ZK (" + path + ")", e);
			return Optional.empty();
		}
	}

	private Optional<String> getConfigurationPath(String jobName) {
		return Optional.ofNullable(jobName)
			.map(jn -> {
				StringTemplate template = new StringTemplate(pathTemplate);
				template.setAttribute(JOB_NAME, jn);
				return template.toString();
			});
	}
}
