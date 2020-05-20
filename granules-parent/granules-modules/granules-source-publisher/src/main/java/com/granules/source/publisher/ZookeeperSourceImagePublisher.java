package com.granules.source.publisher;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.utils.ZKPaths.PathAndNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperSourceImagePublisher extends AbstractSourceImagePublisher implements SourceImagePublisher, Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(ZookeeperSourceImagePublisher.class);

	private static CuratorFramework connect(String connectString) {
		ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(100, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
		client.start();
		return client;
	}

	private final CuratorFramework client;
	private final String watchPath;
	private final String dataPath;
	private final boolean closeClient;
	private final ExecutorService threadPool;
	private static final AtomicInteger THREAD_NUMBER = new AtomicInteger();
	private static final ThreadGroup THREAD_GROUP = new ThreadGroup(ZookeeperSourceImagePublisher.class.getSimpleName() + "_THREADS");
	private static final ThreadFactory THREAD_FACTORY = new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			return new Thread(THREAD_GROUP, r, ZookeeperSourceImagePublisher.class.getSimpleName() + "_THREAD#" + THREAD_NUMBER.incrementAndGet());
		}
	};

	public ZookeeperSourceImagePublisher(String connectString, String watchPath, String dataPath) {
		this(connect(connectString), watchPath, dataPath, null, null, false);
	}

	public ZookeeperSourceImagePublisher(String connectString, String watchPath, String dataPath, Consumer<Exception> errorHandler, Consumer<Exception> warnHandler) {
		this(connect(connectString), watchPath, dataPath, errorHandler, warnHandler, false);
	}

	public ZookeeperSourceImagePublisher(CuratorFramework client, String watchPath, String dataPath) {
		this(client, watchPath, dataPath, null, null, false);
	}

	public ZookeeperSourceImagePublisher(CuratorFramework client, String watchPath, String dataPath, Consumer<Exception> errorHandler, Consumer<Exception> warnHandler) {
		this(client, watchPath, dataPath, errorHandler, warnHandler, false);
	}

	private ZookeeperSourceImagePublisher(CuratorFramework client, String watchPath, String dataPath, Consumer<Exception> errorHandler, Consumer<Exception> warnHandler, boolean closeClient) {
		super(errorHandler, warnHandler);
		this.client = client;
		this.watchPath = watchPath;
		this.dataPath = dataPath;
		this.closeClient = closeClient;
		this.threadPool = Executors.newSingleThreadExecutor(THREAD_FACTORY);
	}

	@Override
	public Future<Void> publish(Integer id, String fqcn, InputStream sourceInputStream) {
		final String stringId = id.toString();
		if (LOG.isDebugEnabled()) {
			LOG.debug("publish. id:{}, fqcn:{}", id, fqcn);
		}
		return threadPool.submit(() -> {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			try (BufferedInputStream in = new BufferedInputStream(sourceInputStream)) {
				byte[] data = new byte[16 * 1024];
				int size;
				while ((size = in.read(data)) > 0) {
					bos.write(data, 0, size);
				}
				setData(ZKPaths.makePath(dataPath, stringId, "FQCN"), fqcn.getBytes());
				setData(ZKPaths.makePath(dataPath, stringId, "JAVA"), bos.toByteArray());
				data = new byte[Long.BYTES];
				putLong(data, 0, System.currentTimeMillis());
				setData(ZKPaths.makePath(watchPath, stringId), data);
			} catch (IOException e) {
				onError(e);
			}
		}, null);
	}

	private void putLong(byte[] b, int off, long val) {
		b[off + 7] = (byte) (val);
		b[off + 6] = (byte) (val >>> 8);
		b[off + 5] = (byte) (val >>> 16);
		b[off + 4] = (byte) (val >>> 24);
		b[off + 3] = (byte) (val >>> 32);
		b[off + 2] = (byte) (val >>> 40);
		b[off + 1] = (byte) (val >>> 48);
		b[off] = (byte) (val >>> 56);
	}

	private void setData(String path, byte[] data) {
		PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
		String parent = pathAndNode.getPath();
		try {
			if (client.checkExists().forPath(path) == null) {
				if (client.checkExists().forPath(parent) == null) {
					createPath(parent);
				}
				client.create().forPath(path, data);
			} else {
				client.setData().forPath(path, data);
			}
		} catch (Exception e) {
			onWarnning(e);
			return;
		}
	}

	private void createPath(String path) throws Exception {
		PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
		String parent = pathAndNode.getPath();
		try {
			if (client.checkExists().forPath(parent) == null) {
				createPath(parent);
			}
			client.create().forPath(path);
		} catch (Exception e) {
			onWarnning(e);
			throw e;
		}
	}

	@Override
	public void close() throws IOException {
		threadPool.shutdown();
		try {
			while (!threadPool.awaitTermination(10, TimeUnit.MILLISECONDS)) {
			}
		} catch (InterruptedException e) {
			onWarnning(e);
		}
		if (closeClient) {
			client.close();
		}
	}

}
