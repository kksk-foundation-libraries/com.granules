package com.granules.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperSourceImageSubscriber extends AbstractSourceImageSubscriber {
	private static final Logger LOG = LoggerFactory.getLogger(ZookeeperSourceImageSubscriber.class);

	private static final AtomicInteger TIMER_SEQUENCE = new AtomicInteger();
	private static final Pattern NUMERIC = Pattern.compile("^[0-9]$");

	private final CuratorFramework client;
	private boolean closeClient;
	private final String watchPath;
	private final int depth;
	private final String dataPath;
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final Timer timer = new Timer(getClass().getSimpleName() + "-Timer-" + TIMER_SEQUENCE.incrementAndGet(), true);

	private static CuratorFramework connect(String connectString) {
		ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(100, 3);
		return CuratorFrameworkFactory.newClient(connectString, retryPolicy);
	}

	public ZookeeperSourceImageSubscriber(String connectString, String watchPath, String dataPath) {
		this(connect(connectString), watchPath, dataPath, null, null, true);
	}

	public ZookeeperSourceImageSubscriber(String connectString, String watchPath, String dataPath, Consumer<Exception> errorHandler, Consumer<Exception> warnHandler) {
		this(connect(connectString), watchPath, dataPath, errorHandler, warnHandler, true);
	}

	public ZookeeperSourceImageSubscriber(CuratorFramework client, String watchPath, String dataPath) {
		this(client, watchPath, dataPath, null, null, false);
	}

	public ZookeeperSourceImageSubscriber(CuratorFramework client, String watchPath, String dataPath, Consumer<Exception> errorHandler, Consumer<Exception> warnHandler) {
		this(client, watchPath, dataPath, errorHandler, warnHandler, false);
	}

	private ZookeeperSourceImageSubscriber(CuratorFramework client, String watchPath, String dataPath, Consumer<Exception> errorHandler, Consumer<Exception> warnHandler, boolean closeClient) {
		super(errorHandler, warnHandler);
		this.client = client;
		this.watchPath = watchPath;
		this.depth = watchPath.split(ZKPaths.PATH_SEPARATOR).length + 1;
		this.dataPath = dataPath;
		this.closeClient = closeClient;
		if (LOG.isDebugEnabled()) {
			LOG.debug("zk conn:[{}], watch path:[{}], data path:[{}], error handling:[{}], warn handling:[{}], close client:[{}]", client.getZookeeperClient().getCurrentConnectionString(), watchPath, dataPath, errorHandler != null, warnHandler != null, closeClient);
		}
	}

	@Override
	public Closeable subscribe() {
		if (closed.get())
			return null;
		TreeCache cache = TreeCache.newBuilder(client, watchPath) //
				.setMaxDepth(depth) //
				.setCacheData(false) //
				.disableZkWatches(false) //
				.build() //
		;

		cache.getListenable().addListener((c, event) -> {
			try {
				TreeCacheEvent.Type type = event.getType();
				if (event.getData() == null)
					return;
				String path = event.getData().getPath();
				if (type == TreeCacheEvent.Type.NODE_ADDED && !watchPath.equals(path)) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("event:{}", event);
					}
					String stringId = ZKPaths.getNodeFromPath(path);
					LOG.debug("loading... id:{}", stringId);
					if (!NUMERIC.matcher(stringId).matches()) {
						onWarnning(new IllegalArgumentException("id must be numeric char:" + stringId));
						return;
					}
					getData(stringId, 0);
				}
			} catch (Exception e) {
				onError(e);
				throw e;
			}
		});

		try {
			cache.start();
		} catch (Exception e) {
			onError(e);
			return null;
		}
		return new Closeable() {
			@Override
			public void close() throws IOException {
				if (closed.get()) {
					return;
				}
				cache.close();
				if (closeClient) {
					client.close();
				}
				closed.set(true);
			}
		};
	}

	private void getData(String stringId, int level) throws Exception {
		Integer id = Integer.valueOf(stringId);
		String fqcn = null;
		String source = null;
		byte[] buf;
		buf = client.getData().forPath(ZKPaths.makePath(dataPath, stringId, "FQCN"));
		if (buf == null) {
			onWarnning(new IllegalArgumentException("the path is required. path:" + ZKPaths.makePath(dataPath, stringId, "FQCN")));
			retry(stringId, level + 1);
			return;
		}
		fqcn = new String(buf);
		buf = client.getData().forPath(ZKPaths.makePath(dataPath, stringId, "JAVA"));
		if (buf == null) {
			onWarnning(new IllegalArgumentException("the path is required. path:" + ZKPaths.makePath(dataPath, stringId, "JAVA")));
			retry(stringId, level + 1);
			return;
		}
		source = new String(buf);
		LocalRegistry.registerSource(id, fqcn, source);
	}

	private void retry(String stringId, int level) {
		if (level >= 10) {
			onWarnning(new Exception("retry over."));
			return;
		}
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				try {
					getData(stringId, level);
				} catch (Exception e) {
					onError(e);
				}
			}
		}, 1_000L);
	}
}
