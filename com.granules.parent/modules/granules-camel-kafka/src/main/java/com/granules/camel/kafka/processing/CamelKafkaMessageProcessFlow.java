package com.granules.camel.kafka.processing;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaManualCommit;
import org.apache.camel.component.reactive.streams.api.CamelReactiveStreams;
import org.apache.ignite.Ignite;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.granules.model.colfer.MessageOffset;
import com.granules.processing.MessageProcessFlow;
import com.granules.processing.MessageProcessor;

import reactor.core.publisher.Flux;

public class CamelKafkaMessageProcessFlow extends MessageProcessFlow {
	private static final Logger LOG = LoggerFactory.getLogger(CamelKafkaMessageProcessFlow.class);

	private final Publisher<Exchange> upstream;

	public CamelKafkaMessageProcessFlow( //
			Ignite cluster, //
			String messageCacheName, //
			String offsetCacheName, //
			String processingCacheName, //
			MessageProcessor messageProcessor, //
			CamelContext camel, //
			String fromKafkaEp) {
		super(cluster, messageCacheName, offsetCacheName, processingCacheName, messageProcessor);
		upstream = CamelReactiveStreams.get(camel).fromStream("CamelKafkaMessageReceiveFlow-from");
		try {
			camel.addRoutes(new RouteBuilder() {
				@Override
				public void configure() throws Exception {
					from(fromKafkaEp).to("reactivestreams:CamelKafkaMessageReceiveFlow-from");
				}
			});
		} catch (Exception e) {
			LOG.error("could not create camel route.");
		}
	}

	@Override
	protected Flux<MessageContext> createProcessorStream() {
		return Flux.from(upstream).map(this::convToMessageContext);
	}

	private MessageContext convToMessageContext(Exchange exchange) {
		MessageContext messageContext = new MessageContext();
		messageContext.header().put(KafkaConstants.MANUAL_COMMIT, exchange.getIn().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class));
		MessageOffset messageOffset = new MessageOffset().unmarshal(exchange.getIn().getBody(byte[].class));
		messageContext.header().put("key", messageOffset.getKey());
		messageContext.messageOffset(messageOffset);
		return messageContext;
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
}
