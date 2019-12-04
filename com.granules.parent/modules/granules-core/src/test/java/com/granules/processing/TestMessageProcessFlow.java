package com.granules.processing;

import java.util.function.Consumer;

import org.apache.ignite.Ignite;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.granules.model.colfer.MessageOffset;

import reactor.core.publisher.Flux;

public class TestMessageProcessFlow extends MessageProcessFlow {
	private static final Logger LOG = LoggerFactory.getLogger(TestMessageProcessFlow.class);

	private Consumer<MessageContext> consumer = null;

	public TestMessageProcessFlow(Ignite cluster, String messageCacheName, String offsetCacheName, String processingCacheName, MessageProcessor messageProcessor) {
		super(cluster, messageCacheName, offsetCacheName, processingCacheName, messageProcessor);
	}
	
	@Override
	protected Flux<MessageContext> createProcessorStream() {
		return Flux.<MessageContext>create(sink -> {
			consumer = messageContext -> {
				sink.next(messageContext);
			};
			sink.onCancel(() -> {
				consumer = null;
			});
		});
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
	protected MessageContext dequeue(MessageContext messageContext) {
		return messageContext;
	}

}
