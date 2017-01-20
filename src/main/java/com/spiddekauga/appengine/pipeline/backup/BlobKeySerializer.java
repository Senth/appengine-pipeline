package com.spiddekauga.appengine.pipeline.backup;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.appengine.api.blobstore.BlobKey;

/**
 * Serialize a {@link com.google.appengine.api.blobstore.BlobKey} into a string
 */
class BlobKeySerializer extends Serializer<BlobKey> {
@Override
public void write(Kryo kryo, Output output, BlobKey object) {
	if (object != null) {
		kryo.writeObjectOrNull(output, object.getKeyString(), String.class);
	} else {
		kryo.writeObjectOrNull(output, null, String.class);
	}
}

@Override
public BlobKey read(Kryo kryo, Input input, Class<BlobKey> type) {
	String keyString = kryo.readObjectOrNull(input, String.class);
	if (keyString != null) {
		return new BlobKey(keyString);
	} else {
		return null;
	}
}
}

