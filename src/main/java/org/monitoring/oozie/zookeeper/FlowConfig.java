package org.monitoring.oozie.zookeeper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.oozie.util.XLog;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class FlowConfig {
	private static final XLog LOGGER = XLog.getLog(FlowConfig.class);
	private static final int TIME_OUT = 10 * 1000;

	private static final Cache<String, Optional<Properties>> CACHE;

	static {
		CACHE = CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).maximumSize(100).build();
	}

	private String zookeeperServer;
	private String zkPathPrefix;

	public FlowConfig(String zookeeperServer, String zkPathPrefix) {
		this.zkPathPrefix = zkPathPrefix;
		this.zookeeperServer = zookeeperServer;
	}

	private Optional<ZooKeeper> initZooKeeperClient() {
		if (zookeeperServer != null) {
			LOGGER.info("Init zookeeper client (" + zookeeperServer + ")");
			final CountDownLatch connectionLatch = new CountDownLatch(1);
			try {
				ZooKeeper zk = new ZooKeeper(zookeeperServer, TIME_OUT, event -> {
					if (event.getState() == KeeperState.SyncConnected) {
						connectionLatch.countDown();
					}
				});
				boolean zkInitiated = connectionLatch.await(TIME_OUT, TimeUnit.MILLISECONDS);
				if (!zkInitiated) {
					throw new IOException("Zk Init failed");
				}
				LOGGER.info("Zookeeper client created");
				return Optional.of(zk);
			} catch (IOException | InterruptedException e) {
				LOGGER.error(e);
			}
		}
		return Optional.empty();
	}

	private Optional<ZooKeeper> checkProps(ZooKeeper zk, String jobPropsPath) {
		Stat stat = null;
		try {
			stat = zk.exists(jobPropsPath, false);
			LOGGER.info("Check (" + jobPropsPath + ") " + stat);
		} catch (KeeperException | InterruptedException e) {
			LOGGER.warn("Check Error (" + jobPropsPath + ")", e);
		}
		return Optional.ofNullable(stat).map(s -> zk);
	}

	public synchronized Optional<Properties> getEventConfiguration(String jobName) {
		Optional<Properties> props = CACHE.getIfPresent(jobName);
		if (props == null) {
			String jobPropsPath = (zkPathPrefix + "/" + jobName).replaceAll("//", "/");
			Optional<ZooKeeper> zk = initZooKeeperClient();

			props = zk.flatMap(z -> checkProps(z, jobPropsPath)).flatMap(z -> getDataFromZk(z, jobPropsPath))
					.map(ByteArrayInputStream::new).flatMap(this::loadProperties);
			zk.ifPresent(this::closeZk);

			CACHE.put(jobName, props);
		}
		return props;
	}

	private Optional<Properties> loadProperties(InputStream jobStream) {
		try {
			Properties props = new Properties();
			props.load(jobStream);
			return Optional.of(props);
		} catch (IOException e) {
			LOGGER.error("Failed to load properties", e);
			return Optional.empty();
		}
	}

	private Optional<byte[]> getDataFromZk(ZooKeeper zk, String jobPropsPath) {
		try {
			LOGGER.info("loading config from (" + jobPropsPath + ")");
			return Optional.of(zk.getData(jobPropsPath, null, null));
		} catch (KeeperException | InterruptedException e) {
			LOGGER.error("Error while getting data from ZK (" + jobPropsPath + ")", e);
			return Optional.empty();
		}
	}

	private void closeZk(ZooKeeper z) {
		try {
			z.close();
		} catch (InterruptedException e) {
			LOGGER.error("Error while closing the zk connection", e);
		}
	}
}
