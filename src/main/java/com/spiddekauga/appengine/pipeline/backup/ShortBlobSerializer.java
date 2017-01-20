package com.spiddekauga.appengine.pipeline.backup;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.appengine.api.datastore.ShortBlob;

/**
 * Serialize Datastore {@link ShortBlob}
 */
public class ShortBlobSerializer extends Serializer<ShortBlob> {
@Override
public void write(Kryo kryo, Output output, ShortBlob object) {
	if (object != null) {
		kryo.writeObjectOrNull(output, object.getBytes(), byte[].class);
	} else {
		kryo.writeObjectOrNull(output, null, byte[].class);
	}
}

@Override
public ShortBlob read(Kryo kryo, Input input, Class<ShortBlob> type) {
	byte[] bytes = kryo.readObjectOrNull(input, byte[].class);
	if (bytes != null) {
		return new ShortBlob(bytes);
	} else {
		return null;
	}
}
}
