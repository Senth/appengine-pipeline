package com.spiddekauga.appengine.pipeline.backup;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.MapJob;
import com.google.appengine.tools.mapreduce.MapSpecification;
import com.google.appengine.tools.mapreduce.inputs.DatastoreInput;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageLevelDbOutput;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.Value;
import com.spiddekauga.appengine.DatastoreUtils;

import java.nio.ByteBuffer;

/**
 * Backup datastore tables to GCS
 */
public class DatastoreBackupJob extends Job0<Void> {
private DatastoreBackupConfig mConfig;

public DatastoreBackupJob(DatastoreBackupConfig config) {
	mConfig = config;
}

@Override
public Value<Void> run() throws Exception {
	JobSetting.WaitForSetting[] waitForExportJobs = new JobSetting.WaitForSetting[mConfig.getBackupTables().size()];

	// Export entities to gcs
	int i = 0;
	for (String table : mConfig.getBackupTables()) {
		FutureValue<?> exportJob = futureCall(new MapJob<>(getMapSpecification(table), mConfig.getMapSettings()), mConfig.getJobSettings());
		waitForExportJobs[i] = new JobSetting.WaitForSetting(exportJob);
		++i;
	}

	// Cleanup temporary files
	return futureCall(new CleanupTempFilesJob(mConfig), mConfig.getJobSettings((JobSetting[]) waitForExportJobs));
}

/**
 * @param table the table to search in
 * @return MapSpecification for saving threads
 */
private MapSpecification<Entity, ByteBuffer, GoogleCloudStorageFileSet> getMapSpecification(String table) {
	Query query = DatastoreUtils.createQuery(table);
	DatastoreInput input = new DatastoreInput(query, mConfig.getShardsPerQuery());
	EntityToKryoMapper mapper = new EntityToKryoMapper();

	// Output
	String filePattern = mConfig.getBackupDirectory() + table + "-%d";
	GoogleCloudStorageLevelDbOutput output = new GoogleCloudStorageLevelDbOutput(mConfig.getGcsBucketName(), filePattern, "binary/octet-stream");

	return new MapSpecification.Builder<>(input, mapper, output)
			.setJobName("Exporting " + table + " to GCS")
			.build();
}

@Override
public String getJobDisplayName() {
	return "Backup Datastore to GCS";
}
}
