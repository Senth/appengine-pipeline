package com.spiddekauga.appengine.pipeline.backup;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.ListOptions;
import com.google.appengine.tools.cloudstorage.ListResult;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.MapJob;
import com.google.appengine.tools.mapreduce.MapSpecification;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLevelDbInput;
import com.google.appengine.tools.mapreduce.outputs.DatastoreOutput;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Value;
import com.spiddekauga.appengine.pipeline.WaitForJob;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Restore the datastore from a previous backup
 */
public class DatastoreRestoreJob extends Job0<Void> {
private static final GcsService mGcsServire = GcsServiceFactory.createGcsService();
private DatastoreBackupConfig mConfig;

public DatastoreRestoreJob(DatastoreBackupConfig config) {
	mConfig = config;
}

@Override
public Value<Void> run() throws Exception {
	FutureValue<?> restoreJob = futureCall(new MapJob<>(getMapSpecification(), mConfig.getMapSettings()), mConfig.getJobSettings());

	return futureCall(new WaitForJob(), restoreJob, mConfig.getJobSettings());
}

/**
 * @return MapSpecification for restoring from GCS to Datastore
 */
private MapSpecification<ByteBuffer, Entity, Void> getMapSpecification() throws Exception {
	GoogleCloudStorageLevelDbInput input = createInput();
	KryoToEntityMapper mapper = new KryoToEntityMapper();
	DatastoreOutput output = new DatastoreOutput();

	return new MapSpecification.Builder<>(input, mapper, output)
			.setJobName("Map Job - Restore from GCS")
			.build();
}

private GoogleCloudStorageLevelDbInput createInput() throws Exception {
	// Get restore files
	ListOptions listOptions = new ListOptions.Builder()
			.setPrefix(mConfig.getBackupDirectory())
			.setRecursive(false)
			.build();
	ListResult listResult = mGcsServire.list(mConfig.getGcsBucketName(), listOptions);
	List<String> filenames = new ArrayList<>();
	while (listResult.hasNext()) {
		filenames.add(listResult.next().getName());
	}

	GoogleCloudStorageFileSet
			restoreFileSet = new GoogleCloudStorageFileSet(mConfig.getGcsBucketName(), filenames);
	return new GoogleCloudStorageLevelDbInput(restoreFileSet);
}

@Override
public String getJobDisplayName() {
	return "Restoring Datastore from GCS";
}
}
