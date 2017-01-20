package com.spiddekauga.appengine.pipeline.backup;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

/**
 * Serialize a {@link Key} into a string
 */
class DatastoreKeySerializer extends Serializer<Key> {
@Override
public void write(Kryo kryo, Output output, Key object) {
	if (object != null) {
		kryo.writeObjectOrNull(output, KeyFactory.keyToString(object), String.class);
	} else {
		kryo.writeObjectOrNull(output, null, String.class);
	}
}

@Override
public Key read(Kryo kryo, Input input, Class<Key> type) {
	String keyString = kryo.readObjectOrNull(input, String.class);
	if (keyString != null) {
		return KeyFactory.stringToKey(keyString);
	} else {
		return null;
	}
}
}
