package org.monitoring.oozie.zookeeper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.commons.lang.StringUtils;
import org.apache.oozie.util.XLog;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.fusesource.hawtbuf.ByteArrayInputStream;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class FlowConfig {
	private static final XLog LOGGER = XLog.getLog(FlowConfig.class);
	private static final int TIME_OUT = 10 * 1000;
	private static final String JOB_NAME = "job_name";
	private static final Cache<String, Optional<Properties>> CACHE;
	
	static {
		CACHE = CacheBuilder.newBuilder()
				.expireAfterAccess(1, TimeUnit.HOURS)
				.build();
	}
	
	private String pathTemplate;
	private String zookeeperServer;
	
	public FlowConfig(String zookeeperServer, String zkPathPrefix) {
		this.zookeeperServer = zookeeperServer;
		pathTemplate = zkPathPrefix + "/$" + JOB_NAME + "$";
	}
	
	private Optional<ZooKeeper> initZooKeeperClient() {
		return Optional.ofNullable(zookeeperServer)
			.map(srv -> {
				LOGGER.info("Init zookeeper client (" + zookeeperServer + ")");
				final CountDownLatch connectionLatch = new CountDownLatch(1);
				try {
					ZooKeeper zk = new ZooKeeper(srv, TIME_OUT, event -> {
						if(event.getState() == KeeperState.SyncConnected) {
							connectionLatch.countDown();
						}
					});
					boolean zkInitiated = connectionLatch.await(TIME_OUT, TimeUnit.MILLISECONDS);
					if(!zkInitiated) {
						throw new IOException("Zk Init failed");
					}
					LOGGER.info("Zookeeper client Created");
					return zk;
				} catch (IOException | InterruptedException e) {
					LOGGER.error(e);
					return null;
				}
			});
	}
	
	public synchronized Optional<Properties> getEventConfiguration(String jobName) {
		Optional<Properties> properties = CACHE.getIfPresent(jobName);
		if(properties == null) {
			properties = getConfigurationPath(jobName).flatMap(this::getPropertiesByPath);
		}
		CACHE.put(jobName, properties);
		return properties;
	}
	
	private Optional<Properties> getPropertiesByPath(String path){
		LOGGER.info("Get properties from (" + path + ")");
		Optional<Properties>  props = initZooKeeperClient()
			.flatMap(zk -> getDataFromZk(zk, path))
			.map(ByteArrayInputStream::new)
			.flatMap(this::loadProperties);
		
		props.ifPresent(p -> LOGGER.info(path + " loaded"));
		
		return props;
	}
	
	private Optional<Properties> loadProperties(InputStream stream){
		try {
			Properties props = new Properties();
			props.load(stream);
			return Optional.of(props);
		} catch (IOException e) {
			LOGGER.error("Failed to load properties", e);
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
			.filter(StringUtils::isNotEmpty)
			.map(jn -> {
				StringTemplate template = new StringTemplate(pathTemplate);
				template.setAttribute(JOB_NAME, jn);
				return template.toString().replace("//", "/");
			});
	}
}
