package org.monitoring.oozie.zookeeper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.oozie.util.XLog;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class FlowConfig {
	private static final XLog LOGGER = XLog.getLog(FlowConfig.class);
	private static final int TIME_OUT = 10 * 1000;
	private static final long CACHE_MAX_TIME = 2 * 60 * 60 * 1000;
	
	private long cacheStartTime;
	private Map<String, Properties> cache;
	private String zookeeperServer;
	private String zkPathPrefix;

	public FlowConfig(String zookeeperServer, String zkPathPrefix) {
		this.zkPathPrefix = zkPathPrefix;
		this.zookeeperServer = zookeeperServer;
		loadJobsProperties();
	}

	private Optional<ZooKeeper> initZooKeeperClient() {
		return Optional.ofNullable(zookeeperServer).map(srv -> {
			LOGGER.info("Init zookeeper client (" + zookeeperServer + ")");
			final CountDownLatch connectionLatch = new CountDownLatch(1);
			try {
				ZooKeeper zk = new ZooKeeper(srv, TIME_OUT, event -> {
					if (event.getState() == KeeperState.SyncConnected) {
						connectionLatch.countDown();
					}
				});
				boolean zkInitiated = connectionLatch.await(TIME_OUT, TimeUnit.MILLISECONDS);
				if (!zkInitiated) {
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

	private void loadJobsProperties() {
		initZooKeeperClient().map(this::loadJobsProperties).ifPresent(newCache -> {
			cache = newCache;
			cacheStartTime = System.currentTimeMillis();
		});
	}

	private Map<String, Properties> loadJobsProperties(ZooKeeper zk) {
		try {
			return getJobsPaths(zk).orElseGet(ArrayList::new).stream().map(path -> this.getDataFromZk(zk, path))
					.filter(Optional::isPresent).map(Optional::get)
					.map(pair -> Pair.of(pair.getLeft(), new ByteArrayInputStream(pair.getRight())))
					.map(pair -> Pair.of(pair.getLeft(), loadProperties(pair.getRight())))
					.filter(pair -> pair.getRight().isPresent())
					.map(pair -> Pair.of(pair.getLeft(), pair.getRight().get()))
					.collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
		} finally {
			try {
				zk.close();
			} catch (InterruptedException e) {
				LOGGER.error("Error while closing zk session " + e.getMessage());
			}
		}

	}

	private Optional<List<String>> getJobsPaths(ZooKeeper zk) {
		try {
			List<String> children = zk.getChildren(zkPathPrefix, false);
			return Optional.ofNullable(children);
		} catch (KeeperException | InterruptedException e) {
			LOGGER.error("Failed to load parent node (" + zkPathPrefix + ")");
			return Optional.empty();
		}
	}

	public synchronized Optional<Properties> getEventConfiguration(String jobName) {
		long newCacheTime = System.currentTimeMillis();
		if(newCacheTime - cacheStartTime > CACHE_MAX_TIME) {
			LOGGER.info("Reload cache");
			loadJobsProperties();
		}
		return Optional.ofNullable(cache.get(jobName));
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

	private Optional<Pair<String, byte[]>> getDataFromZk(ZooKeeper zk, String jobName) {
		String path = (zkPathPrefix + "/" + jobName);
		try {
			LOGGER.info("loading config from (" + path + ")");
			return Optional.of(Pair.of(jobName, zk.getData(path, null, null)));
		} catch (KeeperException | InterruptedException e) {
			LOGGER.error("Error while getting data from ZK (" + path + ")", e);
			return Optional.empty();
		}
	}
}
