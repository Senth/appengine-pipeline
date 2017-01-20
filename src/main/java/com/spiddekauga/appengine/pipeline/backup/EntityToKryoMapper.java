package com.spiddekauga.appengine.pipeline.backup;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.minlog.Log;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.mapreduce.MapOnlyMapper;
import com.spiddekauga.appengine.pipeline.EntitySaveObject;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

/**
 * Saves the Datastore into GCS
 */
class EntityToKryoMapper extends MapOnlyMapper<Entity, ByteBuffer> {
private DatastoreBackupConfig mConfig;
private transient Kryo mKryo = null;

EntityToKryoMapper(DatastoreBackupConfig config) {
	mConfig = config;
}

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
public synchronized void map(Entity entity) {
	Logger logger = Logger.getLogger(getClass().getSimpleName());
	EntitySaveObject saveObject = new EntitySaveObject(entity);

	Log.TRACE();
	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
	Output output = new Output(byteArrayOutputStream);
	mKryo.writeObject(output, saveObject);
	output.close();
	byte[] outputArray = byteArrayOutputStream.toByteArray();
	logger.info("Saved kind: " + entity.getKind() + ", entity: " + KeyFactory.keyToString(saveObject.key) + ", size: " + outputArray.length);
	emit(ByteBuffer.wrap(outputArray));
}
}
