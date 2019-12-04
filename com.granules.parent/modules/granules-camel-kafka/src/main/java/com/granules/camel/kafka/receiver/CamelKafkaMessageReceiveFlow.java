package com.granules.camel.kafka.receiver;

import java.util.function.Consumer;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.ExchangeBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaManualCommit;
import org.apache.camel.component.reactive.streams.api.CamelReactiveStreams;
import org.apache.ignite.Ignite;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.granules.model.colfer.Message;
import com.granules.receiver.MessageReceiveFlow;

import reactor.core.publisher.Flux;

public class CamelKafkaMessageReceiveFlow extends MessageReceiveFlow {
	private static final Logger LOG = LoggerFactory.getLogger(CamelKafkaMessageReceiveFlow.class);

	private final CamelContext camel;
	private final Publisher<Exchange> upstream;
	private Consumer<MessageContext> downstream;

	public CamelKafkaMessageReceiveFlow( //
			Ignite cluster, //
			String messageCacheName, //
			String offsetCacheName, //
			String processingCacheName, //
			CamelContext camel, //
			String fromKafkaEp, //
			String toKafkaEp) {
		super(cluster, messageCacheName, offsetCacheName, processingCacheName);
		this.camel = camel;
		upstream = CamelReactiveStreams.get(camel).fromStream("CamelKafkaMessageReceiveFlow-from");
		Subscriber<Exchange> subscriber = CamelReactiveStreams.get(camel).streamSubscriber("CamelKafkaMessageReceiveFlow-to");

		Flux.<MessageContext>create(sink -> {
			downstream = (messageContext) -> {
				sink.next(messageContext);
			};
			sink.onCancel(() -> {
				downstream = null;
			});
		}) //
				.map(this::convToExchange)//
				.subscribe(subscriber) //
		;

		try {
			camel.addRoutes(new RouteBuilder() {
				@Override
				public void configure() throws Exception {
					from(fromKafkaEp).to("reactivestreams:CamelKafkaMessageReceiveFlow-from");
					from("reactivestreams:CamelKafkaMessageReceiveFlow-to").to(toKafkaEp);
				}
			});
		} catch (Exception e) {
			LOG.error("could not create camel route.");
		}
	}

	@Override
	protected Flux<MessageContext> createReceiverStream() {
		return Flux.from(upstream).map(this::convToMessageContext);
	}

	private MessageContext convToMessageContext(Exchange exchange) {
		MessageContext messageContext = new MessageContext();
		messageContext.header().put(KafkaConstants.MANUAL_COMMIT, exchange.getIn().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class));
		Message message = new Message().unmarshal(exchange.getIn().getBody(byte[].class));
		messageContext.header().put("key", message.getKey());
		messageContext.message(message);
		return messageContext;
	}

	private Exchange convToExchange(MessageContext messageContext) {
		Exchange exchange = ExchangeBuilder.anExchange(camel) //
				.withHeader("key", messageContext.message().getKey()) //
				.withBody(messageContext.messageReceivedOffset().marshal()) //
				.build();
		return exchange;
	}

	@Override
	protected MessageContext dequeue(MessageContext messageContext) {
		// no-op.
		return messageContext;
	}

	@Override
	protected MessageContext ack(MessageContext messageContext) {
		KafkaManualCommit kafkaManualCommit = (KafkaManualCommit) messageContext.header().get(KafkaConstants.MANUAL_COMMIT);
		kafkaManualCommit.commitSync();
		return messageContext;
	}

	@Override
	protected MessageContext enqueue(MessageContext messageContext) {
		downstream.accept(messageContext);
		return messageContext;
	}
}
