package com.granules.serialize;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
@Accessors(fluent = true)
public class Binary {
	private byte[] data;

	public Binary(ColferObject object) {
		this.data = object.marshal();
	}

	public <T extends ColferObject> T mapTo(T target) {
		return target.unmarshal(data);
	}
}
