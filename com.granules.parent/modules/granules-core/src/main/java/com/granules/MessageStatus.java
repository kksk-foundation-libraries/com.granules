package com.granules;

public enum MessageStatus {
	RECEIVED(0), //
	PUBLISHED(1), //
	PROCESSED(2), //
	COMPLETED(3), //
	;

	public final byte code;

	private MessageStatus(int code) {
		this.code = (byte) code;
	}

}
