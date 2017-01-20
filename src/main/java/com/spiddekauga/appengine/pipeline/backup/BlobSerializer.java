package com.spiddekauga.appengine.pipeline.backup;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.appengine.api.datastore.Blob;

/**
 * Serialize Datastore {@link Blob}
 */
public class BlobSerializer extends Serializer<Blob> {
@Override
public void write(Kryo kryo, Output output, Blob object) {
	if (object != null) {
		kryo.writeObjectOrNull(output, object.getBytes(), byte[].class);
	} else {
		kryo.writeObjectOrNull(output, null, byte[].class);
	}
}

@Override
public Blob read(Kryo kryo, Input input, Class<Blob> type) {
	byte[] bytes = kryo.readObjectOrNull(input, byte[].class);
	if (bytes != null) {
		return new Blob(bytes);
	} else {
		return null;
	}
}
}
