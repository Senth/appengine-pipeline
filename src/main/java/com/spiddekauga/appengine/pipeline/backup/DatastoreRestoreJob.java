package com.spiddekauga.appengine.pipeline.backup;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.tools.mapreduce.MapSpecification;
import com.google.appengine.tools.mapreduce.outputs.DatastoreOutput;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Value;
import com.spiddekauga.appengine.pipeline.EntitySaveObject;

/**
 * Restore the datastore from a previous backup
 */
public class DatastoreRestoreJob extends Job1<Void, DatastoreBackupConfig> {
/**
 * @param config datastore backup config
 * @return MapSpecification for restoring from GCS to Datastore
 */
private static MapSpecification<EntitySaveObject, Entity, Void> getMapSpecification(DatastoreBackupConfig config) {
	DatastoreOutput output = new DatastoreOutput();

	return null;
}

@Override
public Value<Void> run(DatastoreBackupConfig config) throws Exception {


	return immediate(null);
}
}
