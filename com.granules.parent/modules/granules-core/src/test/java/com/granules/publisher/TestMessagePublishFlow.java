package com.granules.publisher;

import java.util.function.Consumer;

import org.apache.ignite.Ignite;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.granules.model.colfer.MessageOffset;

import reactor.core.publisher.Flux;

public class TestMessagePublishFlow extends MessagePublishFlow {
	private static final Logger LOG = LoggerFactory.getLogger(TestMessagePublishFlow.class);

	private Consumer<MessageContext> consumer;

	public TestMessagePublishFlow(Ignite cluster, String offsetCacheName, String processingCacheName) {
		super(cluster, offsetCacheName, processingCacheName);
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
		LOG.debug("publish key:[{}], offset:[{}]", PublisherTest.hex(messageContext.messageOffsetPublished().getKey()), messageContext.messageOffsetPublished().getOffset());
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
		LOG.debug("retry key:[{}], offset:[{}]", PublisherTest.hex(messageContext.messageOffset().getKey()), messageContext.messageOffset().getOffset());
		return messageContext;
	}
}
