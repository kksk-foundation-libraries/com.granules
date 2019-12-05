package com.granules.processing;

import java.util.function.Function;

import com.granules.model.colfer.Message;

public interface AggregatedMessageProcessor extends Function<Message[], MessageProcessDirection> {

}
