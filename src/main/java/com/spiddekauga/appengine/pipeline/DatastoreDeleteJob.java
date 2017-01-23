package com.spiddekauga.appengine.pipeline;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.mapreduce.MapJob;
import com.google.appengine.tools.mapreduce.MapReduceResult;
import com.google.appengine.tools.mapreduce.MapSpecification;
import com.google.appengine.tools.mapreduce.inputs.DatastoreInput;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Value;
import com.spiddekauga.appengine.DatastoreUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Deletes all the specified tables from the datastore
 */
public class DatastoreDeleteJob extends Job1<Void, DatastoreJobConfig> {
@Override
public Value<Void> run(DatastoreJobConfig config) throws Exception {
	List<FutureValue<MapReduceResult<Void>>> deleteJobs = new ArrayList<>(config.getTables().size());
	// Delete tables
	for (String table : config.getTables()) {
		deleteJobs.add(futureCall(new MapJob<>(getMapSpecification(table, config), config.getMapSettings()), config.getJobSettings()));
	}

	return futureCall(new WaitForJob(), futureList(deleteJobs), config.getJobSettings());
}

/**
 * @param table the table to search in
 * @param config datastore backup config
 * @return MapSpecification for saving threads
 */
private static MapSpecification<Entity, Void, Void> getMapSpecification(String table, DatastoreJobConfig config) {
	Query query = DatastoreUtils.createQuery(table);
	DatastoreInput input = new DatastoreInput(query, config.getShardsPerQuery());
	DatastoreDeleteMapper mapper = new DatastoreDeleteMapper();

	return new MapSpecification.Builder<Entity, Void, Void>(input, mapper)
			.setJobName("Deleting datastore table: " + table)
			.build();
}
}
