package com.spiddekauga.appengine.pipeline.backup;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.MapJob;
import com.google.appengine.tools.mapreduce.MapReduceResult;
import com.google.appengine.tools.mapreduce.MapSpecification;
import com.google.appengine.tools.mapreduce.inputs.DatastoreInput;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageLevelDbOutput;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.Value;
import com.spiddekauga.appengine.DatastoreUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Backup datastore tables to GCS
 */
public class DatastoreBackupJob extends Job1<Void, DatastoreBackupConfig> {

@Override
public Value<Void> run(DatastoreBackupConfig config) throws Exception {
	List<FutureValue<MapReduceResult<GoogleCloudStorageFileSet>>> exportJobs = new ArrayList<>(config.getBackupTables().size());
	JobSetting.WaitForSetting[] waitForExportJobs = new JobSetting.WaitForSetting[config.getBackupTables().size()];

	// Export entities to gcs
	int i = 0;
	for (String table : config.getBackupTables()) {
		FutureValue<?> exportJob = futureCall(new MapJob<>(getMapSpecification(table, config), config.getMapSettings()), config.getJobSettings());
		waitForExportJobs[i] = new JobSetting.WaitForSetting(exportJob);
		++i;
	}

	// Cleanup temporary files
	futureCall(new CleanupTempFilesJob(config), config.getJobSettings((JobSetting[]) waitForExportJobs));

	return immediate(null);
}

/**
 * @param table the table to search in
 * @param config datastore backup config
 * @return MapSpecification for saving threads
 */
private static MapSpecification<Entity, ByteBuffer, GoogleCloudStorageFileSet> getMapSpecification(String table, DatastoreBackupConfig config) {
	Query query = DatastoreUtils.createQuery(table);
	DatastoreInput input = new DatastoreInput(query, config.getShardsPerQuery());
	EntityToKryoMapper mapper = new EntityToKryoMapper(config);

	// Output
	String filePattern = config.getPrefixDirectory() + table + "-%d";
	GoogleCloudStorageLevelDbOutput output = new GoogleCloudStorageLevelDbOutput(config.getGcsBucketName(), filePattern, "binary/octet-stream");

	return new MapSpecification.Builder<>(input, mapper, output)
			.setJobName("Exporting " + table + " to GCS")
			.build();
}

@Override
public String getJobDisplayName() {
	return "Backup Datastore to GCS";
}
}
