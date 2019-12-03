package com.granules.receiver;

import java.util.function.Consumer;

import org.apache.ignite.Ignite;

import com.granules.model.colfer.Message;

import reactor.core.publisher.Flux;

public class TestMessageReceiveFlow extends MessageReceiveFlow {
	private Consumer<MessageContext> consumer = null;

	public TestMessageReceiveFlow(Ignite cluster, String messageCacheName, String offsetCacheName, String processingCacheName) {
		super(cluster, messageCacheName, offsetCacheName, processingCacheName);
	}

	@Override
	protected Flux<MessageContext> createReceiverStream() {
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

	public void put(Message message) {
		MessageContext messageContext = new MessageContext().message(message);
		consumer.accept(messageContext);
	}
}
