package com.granules.publisher;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.granules.model.Binary;
import com.granules.model.colfer.Message;
import com.granules.model.colfer.MessageOffset;
import com.granules.model.colfer.MessageOffsetKey;

public class PublisherTest {
	static {
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
	}
	static final Logger LOG = LoggerFactory.getLogger(PublisherTest.class);

	@Test
	public void test() {
		Ignite cluster = Ignition.start( //
				new IgniteConfiguration().setCacheConfiguration( //
						new CacheConfiguration<Binary, byte[]>().setName("message").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL), //
						new CacheConfiguration<Binary, Integer>().setName("offset").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL), //
						new CacheConfiguration<Binary, String>().setName("processing").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL) //
				) //
		);
		final String messageCacheName = "message";
		final String offsetCacheName = "offset";
		final String processingCacheName = "processing";

		TestMessagePublishFlow publishFlow = new TestMessagePublishFlow(cluster, offsetCacheName, processingCacheName);
		publishFlow.subscribe(new TestMessagePublishFlow.LocalSubscriber(), new TestMessagePublishFlow.LocalSubscriber());
		TestMessageReceiveFlow receiveFlow = new TestMessageReceiveFlow(cluster, messageCacheName, offsetCacheName, processingCacheName, publishFlow);
		receiveFlow.subscribe(new TestMessageReceiveFlow.LocalSubscriber());
		UUID[] uuids = new UUID[10];

		for (int i = 0; i < 10; i++) {
			uuids[i] = UUID.randomUUID();
		}
		for (int j = 0; j < 10; j++) {
			for (int i = 0; i < 10; i++) {
				UUID uuid = uuids[i];
				byte[] key = new byte[16];
				putLong(key, 0, uuid.getMostSignificantBits());
				putLong(key, 8, uuid.getLeastSignificantBits());
				Message message = new Message() //
						.withMessageType(1) //
						.withKey(key) //
						.withValue(("" + j + ":" + i).getBytes()) //
						.withTimestamp(System.currentTimeMillis()) //
				;
				receiveFlow.put(message);
			}
		}
		final AtomicLong counter = new AtomicLong();
		QueryCursor<Entry<Binary, byte[]>> cursorMessage = cluster.cache("message").query(new ScanQuery<Binary, byte[]>());
		cursorMessage.forEach(entry -> {
			MessageOffset messageOffset = new MessageOffset().unmarshal(entry.getKey().getData());
			Message message = new Message().unmarshal(entry.getValue());
			LOG.debug("message key:[{}], offset:[{}], value:[{}]", hex(message.getKey()), messageOffset.getOffset(), hex(message.getValue()));
			counter.incrementAndGet();
		});
		long messageCount = counter.get();
		LOG.debug("message count:[{}]", messageCount);

		counter.set(0);
		QueryCursor<Entry<Binary, Integer>> cursorOffset = cluster.cache("offset").query(new ScanQuery<Binary, Integer>());
		cursorOffset.forEach(entry -> {
			MessageOffsetKey messageOffsetKey = new MessageOffsetKey().unmarshal(entry.getKey().getData());
			Integer offset = entry.getValue();
			LOG.debug("offset status:[{}], key:[{}], offset:[{}]", hex(messageOffsetKey.getMessageStatus()), hex(messageOffsetKey.getKey()), offset);
			counter.incrementAndGet();
		});
		long offsetCount = counter.get();
		LOG.debug("offset count:[{}]", offsetCount);

		counter.set(0);
		QueryCursor<Entry<Binary, String>> cursorProcessing = cluster.cache("processing").query(new ScanQuery<Binary, String>());
		cursorProcessing.forEach(entry -> {
			MessageOffsetKey messageOffsetKey = new MessageOffsetKey().unmarshal(entry.getKey().getData());
			String proc = entry.getValue();
			LOG.debug("processing status:[{}], key:[{}], process:[{}]", hex(messageOffsetKey.getMessageStatus()), hex(messageOffsetKey.getKey()), proc);
			counter.incrementAndGet();
		});
		long processingCount = counter.get();
		LOG.debug("processing count:[{}]", processingCount);
	}

	static String hex(byte... data) {
		StringBuilder sb = new StringBuilder();
		for (byte d : data) {
			sb.append(String.format("%02x", d));
		}
		return sb.toString();
	}

	static void putLong(byte[] b, int off, long val) {
		b[off + 7] = (byte) (val);
		b[off + 6] = (byte) (val >>> 8);
		b[off + 5] = (byte) (val >>> 16);
		b[off + 4] = (byte) (val >>> 24);
		b[off + 3] = (byte) (val >>> 32);
		b[off + 2] = (byte) (val >>> 40);
		b[off + 1] = (byte) (val >>> 48);
		b[off] = (byte) (val >>> 56);
	}

}
