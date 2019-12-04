package com.granules.camel.kafka.publisher;

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

import com.granules.model.colfer.MessageOffset;
import com.granules.publisher.MessagePublishFlow;

import reactor.core.publisher.Flux;

public class CamelKafkaMessagePublishFlow extends MessagePublishFlow {
	private static final Logger LOG = LoggerFactory.getLogger(CamelKafkaMessagePublishFlow.class);

	private final CamelContext camel;
	private final Publisher<Exchange> upstream;
	private Consumer<MessageContext> downstream;

	public CamelKafkaMessagePublishFlow( //
			Ignite cluster, //
			String offsetCacheName, //
			String processingCacheName, //
			CamelContext camel, //
			String fromKafkaEp, //
			String toKafkaEp) {
		super(cluster, offsetCacheName, processingCacheName);
		this.camel = camel;
		this.upstream = CamelReactiveStreams.get(camel).fromStream("CamelKafkaMessagePublishFlow-from");
		Subscriber<Exchange> subscriber = CamelReactiveStreams.get(camel).streamSubscriber("CamelKafkaMessagePublishFlow-to");

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
					from(fromKafkaEp).to("reactivestreams:CamelKafkaMessagePublishFlow-from");
					from("reactivestreams:CamelKafkaMessagePublishFlow-to").to(toKafkaEp);
				}
			});
		} catch (Exception e) {
			LOG.error("could not create camel route.");
		}
	}

	private MessageContext convToMessageContext(Exchange exchange) {
		MessageContext messageContext = new MessageContext();
		messageContext.header().put(KafkaConstants.MANUAL_COMMIT, exchange.getIn().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class));
		MessageOffset messageOffset = new MessageOffset().unmarshal(exchange.getIn().getBody(byte[].class));
		messageContext.header().put("key", messageOffset.getKey());
		messageContext.messageOffset(messageOffset);
		return messageContext;
	}

	private Exchange convToExchange(MessageContext messageContext) {
		Exchange exchange = ExchangeBuilder.anExchange(camel) //
				.withHeader("key", messageContext.messageOffsetPublished().getKey()) //
				.withBody(messageContext.messageOffsetPublished().marshal()) //
				.build();
		return exchange;
	}

	@Override
	protected Flux<MessageContext> createPublisherStream() {
		return Flux.from(upstream).map(this::convToMessageContext);
	}

	@Override
	protected MessageContext dequeue(MessageContext messageContext) {
		return messageContext;
	}

	@Override
	protected MessageContext ack(MessageContext messageContext) {
		KafkaManualCommit kafkaManualCommit = (KafkaManualCommit) messageContext.header().get(KafkaConstants.MANUAL_COMMIT);
		kafkaManualCommit.commitSync();
		return messageContext;
	}

	@Override
	protected MessageContext publish(MessageContext messageContext) {
		downstream.accept(messageContext);
		return messageContext;
	}

}
