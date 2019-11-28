package com.granules.processing;

import java.lang.management.ManagementFactory;
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

public abstract class MessageProcessFlow {
	private static final Logger LOG = LoggerFactory.getLogger(MessageProcessFlow.class);
	private static final String PROCESS_ID = ManagementFactory.getRuntimeMXBean().getName() + ":" + MessageProcessFlow.class.getSimpleName();

	private final IgniteCache<Binary, byte[]> messageCache;
	private final IgniteCache<Binary, Integer> offsetCache;
	private final IgniteCache<Binary, String> processingCache;
	private final MessageProcessor messageProcessor;

	public MessageProcessFlow(Ignite cluster, String messageCacheName, String offsetCacheName, String processingCacheName, MessageProcessor messageProcessor) {
		this.messageCache = cluster.cache(messageCacheName);
		this.offsetCache = cluster.cache(offsetCacheName);
		this.processingCache = cluster.cache(processingCacheName);
		this.messageProcessor = messageProcessor;
		if (LOG.isDebugEnabled()) {
			LOG.debug("constructed. messageCache:[{}], offsetCache:[{}], offsetCache:[{}]", messageCacheName, offsetCacheName, processingCacheName);
		}
	}

	protected abstract Flux<MessageContext> createProcessorStream();

	protected abstract MessageContext dequeue(MessageContext messageContext);

	protected MessageContext ack(MessageContext messageContext) {
		return messageContext;
	}

	protected MessageContext process(MessageContext messageContext) {
		MessageProcessDirection messageProcessDirection = messageProcessor.apply(messageContext.message());
		return messageContext.messageProcessDirection(messageProcessDirection);
	}

	protected MessageContext onSucceed(MessageContext messageContext) {
		return onComplete(messageContext);
	}

	protected MessageContext onRetry(MessageContext messageContext) {
		return messageContext;
	}

	protected MessageContext onSuspend(MessageContext messageContext) {
		return messageContext;
	}

	protected MessageContext onSkip(MessageContext messageContext) {
		return onComplete(messageContext);
	}

	protected MessageContext onComplete(MessageContext messageContext) {
		return messageContext;
	}

	public void subscribe(Subscriber<MessageContext> succeedSubscriber, Subscriber<MessageContext> retrySubscriber, Subscriber<MessageContext> suspendSubscriber, Subscriber<MessageContext> skipSubscriber) {
		Flux<MessageContext> upstream = //
				createProcessorStream() //
						.map(this::dequeue) //
						.map(this::createMessageOffsetKey) //
						.map(this::lock) //
						.map(this::begin) //
						.map(this::ack) //
						.map(this::getMessage) //
						.map(this::process) //
		;
		upstream //
				.filter(this::isSucceed) //
				.map(this::onSucceed) //
				.map(this::saveMessageOffsetKey) //
				.map(this::end) //
				.map(this::unlock) //
				.subscribe(succeedSubscriber) //
		;
		upstream //
				.filter(this::isRetry) //
				.map(this::onRetry) //
				.map(this::end) //
				.map(this::unlock) //
				.subscribe(retrySubscriber) //
		;
		upstream //
				.filter(this::isSuspend) //
				.map(this::onSuspend) //
				.map(this::end) //
				.map(this::unlock) //
				.subscribe(suspendSubscriber) //
		;
		upstream //
				.filter(this::isSkip) //
				.map(this::onSkip) //
				.map(this::end) //
				.map(this::unlock) //
				.subscribe(skipSubscriber) //
		;
	}

	private MessageContext createMessageOffsetKey(MessageContext messageContext) {
		MessageOffset messageOffset = messageContext.messageOffset();
		MessageOffsetKey messageOffsetKey = //
				new MessageOffsetKey() //
						.withMessageStatus(MessageStatus.PROCESSED.code) //
						.withMessageType(messageOffset.getMessageType()) //
						.withKey(messageOffset.getKey()) //
		;
		return messageContext.binaryMessageOffsetKey(Binary.of(messageOffsetKey));
	}

	private MessageContext begin(MessageContext messageContext) {
		Binary binaryMessageOffsetKey = messageContext.binaryMessageOffsetKey();
		processingCache.put(binaryMessageOffsetKey, PROCESS_ID);
		return messageContext;
	}

	private MessageContext unlock(MessageContext messageContext) {
		Lock lock = messageContext.lock();
		lock.unlock();
		return messageContext;
	}

	private MessageContext getMessage(MessageContext messageContext) {
		byte[] value = messageCache.get(Binary.of(messageContext.messageOffset()));
		if (value != null) {
			messageContext.message(new Message().unmarshal(value));
		}
		return messageContext;
	}

	private boolean isSucceed(MessageContext messageContext) {
		return messageContext.messageProcessDirection() == MessageProcessDirection.SUCCEED;
	}

	private boolean isRetry(MessageContext messageContext) {
		return messageContext.messageProcessDirection() == MessageProcessDirection.RETRY;
	}

	private boolean isSuspend(MessageContext messageContext) {
		return messageContext.messageProcessDirection() == MessageProcessDirection.SUSPEND;
	}

	private boolean isSkip(MessageContext messageContext) {
		return messageContext.messageProcessDirection() == MessageProcessDirection.SKIP;
	}

	private MessageContext lock(MessageContext messageContext) {
		Binary binaryMessageOffsetKey = messageContext.binaryMessageOffsetKey();
		Lock lock = offsetCache.lock(binaryMessageOffsetKey);
		return messageContext.lock(lock);
	}

	private MessageContext end(MessageContext messageContext) {
		Binary binaryMessageOffsetKey = messageContext.binaryMessageOffsetKey();
		processingCache.remove(binaryMessageOffsetKey);
		return messageContext;
	}

	private MessageContext saveMessageOffsetKey(MessageContext messageContext) {
		MessageOffset messageOffset = messageContext.messageOffset();
		Binary binaryMessageOffsetKey = messageContext.binaryMessageOffsetKey();
		offsetCache.put(binaryMessageOffsetKey, messageOffset.getOffset());
		return messageContext;
	}

	@Data
	@Accessors(fluent = true)
	protected static class MessageContext {
		private MessageOffset messageOffset;
		private Message message;
		@Getter(value = AccessLevel.PRIVATE)
		@Setter(value = AccessLevel.PRIVATE)
		private Binary binaryMessageOffsetKey;
		@Getter(value = AccessLevel.PRIVATE)
		@Setter(value = AccessLevel.PRIVATE)
		private Lock lock;
		private MessageProcessDirection messageProcessDirection;
		private ConcurrentMap<String, ?> header;
	}

}
