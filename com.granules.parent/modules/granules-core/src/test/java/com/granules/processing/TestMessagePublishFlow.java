package com.granules.processing;

import java.util.function.Consumer;

import org.apache.ignite.Ignite;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.granules.model.colfer.MessageOffset;
import com.granules.publisher.MessagePublishFlow;

import reactor.core.publisher.Flux;

public class TestMessagePublishFlow extends MessagePublishFlow {
	private static final Logger LOG = LoggerFactory.getLogger(TestMessagePublishFlow.class);

	private Consumer<MessageContext> consumer;
	private final TestMessageProcessFlow processFlow;

	public TestMessagePublishFlow(Ignite cluster, String offsetCacheName, String processingCacheName, TestMessageProcessFlow processFlow) {
		super(cluster, offsetCacheName, processingCacheName);
		this.processFlow = processFlow;
	}

	@Override
	protected Flux<MessageContext> createPublisherStream() {
		return Flux.<MessageContext>create(sink -> {
			consumer = messageContext -> {
				sink.next(messageContext);
			};
			sink.onCancel(() -> {
				consumer = null;
			});
		});
	}

	@Override
	protected MessageContext dequeue(MessageContext messageContext) {
		return messageContext;
	}

	@Override
	protected MessageContext publish(MessageContext messageContext) {
		LOG.debug("publish key:[{}], offset:[{}]", hex(messageContext.messageOffset().getKey()), messageContext.messageOffset().getOffset());
		processFlow.put(messageContext.messageOffsetPublished());
		return messageContext;
	}

	public void put(MessageOffset messageOffset) {
		consumer.accept(new MessageContext().messageOffset(messageOffset));
	}

	static class LocalSubscriber implements Subscriber<MessageContext> {
		private Subscription upstream;

		@Override
		public void onSubscribe(Subscription s) {
			upstream = s;
			upstream.request(1);
		}

		@Override
		public void onNext(MessageContext t) {
			upstream.request(1);
		}

		@Override
		public void onError(Throwable t) {
			upstream.cancel();
			LOG.error("error!", t);
		}

		@Override
		public void onComplete() {
			LOG.debug("completed!");
		}

	}

	@Override
	protected MessageContext retry(MessageContext messageContext) {
		LOG.debug("retry key:[{}], offset:[{}]", hex(messageContext.messageOffset().getKey()), messageContext.messageOffset().getOffset());
		return messageContext;
	}

	static String hex(byte... data) {
		StringBuilder sb = new StringBuilder();
		for (byte d : data) {
			sb.append(String.format("%02x", d));
		}
		return sb.toString();
	}
}
