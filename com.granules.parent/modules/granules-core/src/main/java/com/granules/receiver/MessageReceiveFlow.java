package com.granules.receiver;

import java.lang.management.ManagementFactory;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.granules.MessageStatus;
import com.granules.model.Binary;
import com.granules.model.colfer.Message;
import com.granules.model.colfer.MessageOffset;
import com.granules.model.colfer.MessageOffsetKey;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import reactor.core.publisher.Flux;

public abstract class MessageReceiveFlow {
	@Data
	@Accessors(fluent = true)
	protected static class MessageContext {
		private Message message;
		@Getter(value = AccessLevel.PRIVATE)
		@Setter(value = AccessLevel.PRIVATE)
		private Binary binaryReceivedMessageOffsetKey;
		@Getter(value = AccessLevel.PRIVATE)
		@Setter(value = AccessLevel.PRIVATE)
		private Lock lock;
		@Getter(value = AccessLevel.PRIVATE)
		@Setter(value = AccessLevel.PRIVATE)
		private int currentReceivedOffset;
		@Getter(value = AccessLevel.PRIVATE)
		@Setter(value = AccessLevel.PRIVATE)
		private int nextReceivedOffset;
		private MessageOffset messageReceivedOffset;
		private final ConcurrentMap<String, Object> header = new ConcurrentHashMap<>();
	}
	private static final Logger LOG = LoggerFactory.getLogger(MessageReceiveFlow.class);

	private static final String PROCESS_ID = ManagementFactory.getRuntimeMXBean().getName() + ":" + MessageReceiveFlow.class.getSimpleName();
	private final IgniteCache<Binary, byte[]> messageCache;
	private final IgniteCache<Binary, Integer> offsetCache;

	private final IgniteCache<Binary, String> processingCache;

	public MessageReceiveFlow(Ignite cluster, String messageCacheName, String offsetCacheName, String processingCacheName) {
		this.messageCache = cluster.cache(messageCacheName);
		this.offsetCache = cluster.cache(offsetCacheName);
		this.processingCache = cluster.cache(processingCacheName);
		if (LOG.isDebugEnabled()) {
			LOG.debug("constructed. messageCache:[{}], offsetCache:[{}], processingCache:[{}]", messageCacheName, offsetCacheName, processingCacheName);
		}
	}

	protected MessageContext ack(MessageContext messageContext) {
		// no-op
		return messageContext;
	}

	private MessageContext begin(MessageContext messageContext) {
		Binary binaryMessageOffsetKey = messageContext.binaryReceivedMessageOffsetKey();
		processingCache.put(binaryMessageOffsetKey, PROCESS_ID);
		return messageContext;
	}

	private MessageContext createReceivedMessageOffset(MessageContext messageContext) {
		Message message = messageContext.message();
		int nextOffset = messageContext.nextReceivedOffset();
		MessageOffset messageOffset = //
				new MessageOffset() //
						.withMessageType(message.getMessageType()) //
						.withKey(message.getKey()) //
						.withOffset(nextOffset) //
		;
		return messageContext.messageReceivedOffset(messageOffset);
	}

	private MessageContext createReceivedMessageOffsetKey(MessageContext messageContext) {
		Message message = messageContext.message();
		MessageOffsetKey messageOffsetKey = //
				new MessageOffsetKey() //
						.withMessageStatus(MessageStatus.RECEIVED.code) //
						.withMessageType(message.getMessageType()) //
						.withKey(message.getKey()) //
		;
		return messageContext.binaryReceivedMessageOffsetKey(Binary.of(messageOffsetKey));
	}

	protected abstract Flux<MessageContext> createReceiverStream();

	protected abstract MessageContext dequeue(MessageContext messageContext);

	private MessageContext end(MessageContext messageContext) {
		Binary binaryMessageOffsetKey = messageContext.binaryReceivedMessageOffsetKey();
		processingCache.remove(binaryMessageOffsetKey);
		return messageContext;
	}

	protected MessageContext enqueue(MessageContext messageContext) {
		// no-op
		return messageContext;
	}

	private MessageContext getReceivedCurrentOffset(MessageContext messageContext) {
		Binary binaryMessageOffsetKey = messageContext.binaryReceivedMessageOffsetKey();
		Integer currentOffset = offsetCache.get(binaryMessageOffsetKey);
		if (currentOffset == null) {
			currentOffset = 0;
		}
		return messageContext.currentReceivedOffset(currentOffset);
	}

	private MessageContext getReceivedNextOffset(MessageContext messageContext) {
		int currentOffset = messageContext.currentReceivedOffset();
		int nextOffset = currentOffset + 1;
		return messageContext.nextReceivedOffset(nextOffset);
	}

	private MessageContext lock(MessageContext messageContext) {
		Binary binaryMessageOffsetKey = messageContext.binaryReceivedMessageOffsetKey();
		Lock lock = offsetCache.lock(binaryMessageOffsetKey);
		lock.lock();
		return messageContext.lock(lock);
	}

	private MessageContext saveMessage(MessageContext messageContext) {
		Message message = messageContext.message();
		MessageOffset messageOffset = messageContext.messageReceivedOffset();
		messageCache.put(Binary.of(messageOffset), message.withOffset(messageContext.messageReceivedOffset().getOffset()).marshal());
		return messageContext;
	}

	private MessageContext saveReceivedNextOffset(MessageContext messageContext) {
		Binary binaryMessageOffsetKey = messageContext.binaryReceivedMessageOffsetKey();
		int nextOffset = messageContext.nextReceivedOffset();
		offsetCache.put(binaryMessageOffsetKey, nextOffset);
		return messageContext;
	}

	public void subscribe(Subscriber<MessageContext> receivedMessageSubscriber) {
		createReceiverStream() //
				.map(this::dequeue) //
				.map(this::createReceivedMessageOffsetKey) //
				.map(this::lock) //
				.map(this::begin) //
				.map(this::getReceivedCurrentOffset) //
				.map(this::getReceivedNextOffset) //
				.map(this::createReceivedMessageOffset) //
				.map(this::saveMessage) //
				.map(this::ack) //
				.map(this::enqueue) //
				.map(this::saveReceivedNextOffset) //
				.map(this::end) //
				.map(this::unlock) //
				.subscribe(receivedMessageSubscriber) //
		;
	}

	private MessageContext unlock(MessageContext messageContext) {
		Lock lock = messageContext.lock();
		lock.unlock();
		return messageContext;
	}

}
