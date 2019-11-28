package com.granules;

public enum MessageStatus {
	RECEIVED(0), //
	DEQUEUED(1), //
	PUBLISHED(2), //
	PROCESSED(3), //
	COMPLETED(4), //
	;

	public final byte code;

	private MessageStatus(int code) {
		this.code = (byte) code;
	}

}
