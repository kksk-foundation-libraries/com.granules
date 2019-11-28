package com.granules.processing;

import java.util.function.Function;

import com.granules.model.colfer.Message;

public interface MessageProcessor extends Function<Message, MessageProcessDirection> {

}
