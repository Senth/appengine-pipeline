package com.spiddekauga.appengine.pipeline.backup;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.tools.mapreduce.MapOnlyMapper;
import com.spiddekauga.appengine.pipeline.EntitySaveObject;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

/**
 * Converts a kryo object into a datastore object
 */
class KryoToEntityMapper extends MapOnlyMapper<ByteBuffer, Entity> {
private transient Kryo mKryo = null;

@Override
public void beginShard() {
	mKryo = KryoFactory.create();
}

@Override
public void beginSlice() {
	if (mKryo == null) {
		mKryo = KryoFactory.create();
	}
}

@Override
public void map(ByteBuffer byteBuffer) {
	ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteBuffer.array());
	Input input = new Input(byteArrayInputStream);
	EntitySaveObject saveObject = mKryo.readObject(input, EntitySaveObject.class);
	input.close();

	if (saveObject != null) {
		emit(saveObject.toDatastoreEntity());
	} else {
		emit(null);
	}
}
}
