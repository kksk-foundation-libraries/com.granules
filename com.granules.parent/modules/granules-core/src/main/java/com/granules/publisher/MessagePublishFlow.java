package com.granules.publisher;

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
import com.granules.model.colfer.MessageOffset;
import com.granules.model.colfer.MessageOffsetKey;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import reactor.core.publisher.Flux;

public abstract class MessagePublishFlow {
	private static final Logger LOG = LoggerFactory.getLogger(MessagePublishFlow.class);
	private static final String PROCESS_ID = ManagementFactory.getRuntimeMXBean().getName() + ":" + MessagePublishFlow.class.getSimpleName();

	private final IgniteCache<Binary, Integer> offsetCache;
	private final IgniteCache<Binary, String> processingCache;

	public MessagePublishFlow(Ignite cluster, String offsetCacheName, String processingCacheName) {
		this.offsetCache = cluster.cache(offsetCacheName);
		this.processingCache = cluster.cache(processingCacheName);
		if (LOG.isDebugEnabled()) {
			LOG.debug("constructed. offsetCache:[{}], processingCache:[{}]", offsetCacheName, processingCacheName);
		}
	}

	protected abstract Flux<MessageContext> createPublisherStream();

	protected abstract MessageContext dequeue(MessageContext messageContext);

	protected MessageContext ack(MessageContext messageContext) {
		return messageContext;
	}

	protected abstract MessageContext publish(MessageContext messageContext);

	protected MessageContext retry(MessageContext messageContext) {
		return messageContext;
	}

	public void subscribe(Subscriber<MessageContext> publishedMessageSubscriber, Subscriber<MessageContext> retryedMessageSubscriber) {
		Flux<MessageContext> upstream = //
				createPublisherStream() //
						.map(this::dequeue) //
						.map(this::createMessageOffsetKey) //
						.map(this::lock) //
						.map(this::begin) //
						.map(this::ack) //
						.map(this::createProcessedMessageOffsetKey) //
						.map(this::getPublishedCurrentOffset) //
						.map(this::getProcessedCurrentOffset) //
		;
		upstream //
				.filter(this::isPublishable) //
				.map(this::getPublishedNextOffset) //
				.map(this::updateMessageOffset) //
				.map(this::publish) //
				.map(this::savePublishedNextOffset) //
				.map(this::end) //
				.map(this::unlock) //
				.subscribe(publishedMessageSubscriber) //
		;
		upstream //
				.filter(this::isNotPublishable) //
				.map(this::retry) //
				.map(this::end) //
				.map(this::unlock) //
				.subscribe(retryedMessageSubscriber) //
		;
	}

	private MessageContext createMessageOffsetKey(MessageContext messageContext) {
		MessageOffset messageOffset = messageContext.messageOffset();
		MessageOffsetKey messageOffsetKey = //
				new MessageOffsetKey() //
						.withMessageStatus(MessageStatus.PUBLISHED.code) //
						.withMessageType(messageOffset.getMessageType()) //
						.withKey(messageOffset.getKey()) //
		;
		return messageContext.binaryMessageOffsetKey(Binary.of(messageOffsetKey));
	}

	private MessageContext lock(MessageContext messageContext) {
		Binary binaryMessageOffsetKey = messageContext.binaryMessageOffsetKey();
		Lock lock = offsetCache.lock(binaryMessageOffsetKey);
		return messageContext.lock(lock);
	}

	private MessageContext begin(MessageContext messageContext) {
		Binary binaryMessageOffsetKey = messageContext.binaryMessageOffsetKey();
		processingCache.put(binaryMessageOffsetKey, PROCESS_ID);
		return messageContext;
	}

	private MessageContext createProcessedMessageOffsetKey(MessageContext messageContext) {
		MessageOffset messageOffset = messageContext.messageOffset();
		MessageOffsetKey messageOffsetKey = //
				new MessageOffsetKey() //
						.withMessageStatus(MessageStatus.PROCESSED.code) //
						.withMessageType(messageOffset.getMessageType()) //
						.withKey(messageOffset.getKey()) //
		;
		return messageContext.binaryMessageOffsetKeyProcessed(Binary.of(messageOffsetKey));
	}

	private MessageContext getPublishedCurrentOffset(MessageContext messageContext) {
		Binary binaryMessageOffsetKey = messageContext.binaryMessageOffsetKey();
		Integer currentOffset = offsetCache.get(binaryMessageOffsetKey);
		if (currentOffset == null) {
			currentOffset = 0;
		}
		return messageContext.currentPublishedOffset(currentOffset);
	}

	private MessageContext getProcessedCurrentOffset(MessageContext messageContext) {
		Binary binaryMessageOffsetKey = messageContext.binaryMessageOffsetKeyProcessed();
		Integer currentOffset = offsetCache.get(binaryMessageOffsetKey);
		if (currentOffset == null) {
			currentOffset = 0;
		}
		return messageContext.currentProcessedOffset(currentOffset);
	}

	private boolean isPublishable(MessageContext messageContext) {
		return messageContext.currentPublishedOffset() == messageContext.currentProcessedOffset();
	}

	private boolean isNotPublishable(MessageContext messageContext) {
		return messageContext.currentPublishedOffset() != messageContext.currentProcessedOffset();
	}

	private MessageContext getPublishedNextOffset(MessageContext messageContext) {
		int currentOffset = messageContext.currentPublishedOffset();
		int nextOffset = currentOffset + 1;
		return messageContext.nextPublishedOffset(nextOffset);
	}

	private MessageContext updateMessageOffset(MessageContext messageContext) {
		MessageOffset lastMessageOffset = messageContext.messageOffset();
		int nextOffset = messageContext.nextPublishedOffset();
		MessageOffset messageOffset = lastMessageOffset;
		if (messageOffset.getOffset() != nextOffset) {
			messageOffset = //
					new MessageOffset() //
							.withMessageType(lastMessageOffset.getMessageType()) //
							.withKey(lastMessageOffset.getKey()) //
							.withOffset(nextOffset) //
			;
		}
		return messageContext.messageOffsetPublished(messageOffset);
	}

	private MessageContext savePublishedNextOffset(MessageContext messageContext) {
		Binary binaryMessageOffsetKey = messageContext.binaryMessageOffsetKey();
		int nextOffset = messageContext.nextPublishedOffset();
		offsetCache.put(binaryMessageOffsetKey, nextOffset);
		return messageContext;
	}

	private MessageContext end(MessageContext messageContext) {
		Binary binaryMessageOffsetKey = messageContext.binaryMessageOffsetKey();
		processingCache.remove(binaryMessageOffsetKey);
		return messageContext;
	}

	private MessageContext unlock(MessageContext messageContext) {
		Lock lock = messageContext.lock();
		lock.unlock();
		return messageContext;
	}

	@Data
	@Accessors(fluent = true)
	protected static class MessageContext {
		private MessageOffset messageOffset;
		@Getter(value = AccessLevel.PRIVATE)
		@Setter(value = AccessLevel.PRIVATE)
		private Binary binaryMessageOffsetKey;
		@Getter(value = AccessLevel.PRIVATE)
		@Setter(value = AccessLevel.PRIVATE)
		private Binary binaryMessageOffsetKeyProcessed;
		@Getter(value = AccessLevel.PRIVATE)
		@Setter(value = AccessLevel.PRIVATE)
		private Lock lock;
		@Getter(value = AccessLevel.PRIVATE)
		@Setter(value = AccessLevel.PRIVATE)
		private int currentPublishedOffset;
		@Getter(value = AccessLevel.PRIVATE)
		@Setter(value = AccessLevel.PRIVATE)
		private int currentProcessedOffset;
		@Getter(value = AccessLevel.PRIVATE)
		@Setter(value = AccessLevel.PRIVATE)
		private int nextPublishedOffset;
		private MessageOffset messageOffsetPublished;
		private final ConcurrentMap<String, Object> header = new ConcurrentHashMap<>();
	}

}
