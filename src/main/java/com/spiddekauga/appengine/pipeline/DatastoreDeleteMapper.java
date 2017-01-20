package com.spiddekauga.appengine.pipeline;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.tools.mapreduce.DatastoreMutationPool;
import com.google.appengine.tools.mapreduce.MapOnlyMapper;

/**
 * Deletes all results from the datastore query
 */
public class DatastoreDeleteMapper extends MapOnlyMapper<Entity, Void> {
private transient DatastoreMutationPool mDatastorePool = null;

@Override
public void beginShard() {
	mDatastorePool = DatastoreMutationPool.create();
}

@Override
public void endShard() {
	mDatastorePool.flush();
}

@Override
public void map(Entity value) {
	mDatastorePool.delete(value.getKey());
}
}
