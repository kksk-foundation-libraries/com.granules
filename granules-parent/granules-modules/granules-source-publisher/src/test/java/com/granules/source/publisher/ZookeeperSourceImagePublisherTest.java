package com.granules.source.publisher;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.granules.test.EmbeddedZookeeperLauncher;
import com.granules.test.SimpleLogging;

public class ZookeeperSourceImagePublisherTest {
	@ClassRule
	public static SimpleLogging simpleLogging = SimpleLogging.getInstance("com.granules");

	private static final Logger LOG = LoggerFactory.getLogger(ZookeeperSourceImagePublisherTest.class);

	@Rule
	public EmbeddedZookeeperLauncher zk = new EmbeddedZookeeperLauncher();

	@Test
	public void test() {
		LOG.debug(zk.connectString());
		LOG.debug("begin--------------------------------");
		ZookeeperSourceImagePublisher publisher = new ZookeeperSourceImagePublisher(zk.connectString(), "/TEST/WATCH", "/TEST/DATA");
		LOG.debug("test---------------------------------");
		Future<Void> future = publisher.publish(1, "f.q.c.n", "source");
		try {
			future.get();
		} catch (InterruptedException e) {
			LOG.error("InterruptedException", e);
			return;
		} catch (ExecutionException e) {
			LOG.error("ExecutionException", e);
			fail(e.getMessage());
			return;
		}
		LOG.debug("end----------------------------------");
		try {
			publisher.close();
			LOG.debug("closed-------------------------------");
		} catch (IOException e) {
			LOG.error("IOException", e);
			fail(e.getMessage());
		}
	}

}
