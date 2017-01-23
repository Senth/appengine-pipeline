package com.spiddekauga.appengine.pipeline.backup;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.tools.mapreduce.MapOnlyMapper;
import com.spiddekauga.appengine.pipeline.EntitySaveObject;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/**
 * Converts a datastore object to a kryo object that can be saved
 */
class EntityToKryoMapper extends MapOnlyMapper<Entity, ByteBuffer> {
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
public void map(Entity entity) {
//	Logger logger = Logger.getLogger(getClass().getSimpleName());
	EntitySaveObject saveObject = new EntitySaveObject(entity);

//	Log.TRACE();
	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
	Output output = new Output(byteArrayOutputStream);
	mKryo.writeObject(output, saveObject);
	output.close();
	byte[] outputArray = byteArrayOutputStream.toByteArray();
//	logger.info("Saved kind: " + entity.getKind() + ", entity: " + KeyFactory.keyToString(saveObject.key) + ", size: " + outputArray.length);
	emit(ByteBuffer.wrap(outputArray));
}
}
