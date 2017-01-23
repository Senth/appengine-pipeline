package com.spiddekauga.appengine.pipeline.backup;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.ListItem;
import com.google.appengine.tools.cloudstorage.ListOptions;
import com.google.appengine.tools.cloudstorage.ListResult;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Value;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Remove temporary files when jobs are complete
 */
class CleanupTempFilesJob extends Job0<Void> {
private static final Pattern TEMP_FILE_PATTERN = Pattern.compile(".+~.*");
private static final GcsService mGcsService = GcsServiceFactory.createGcsService();
private DatastoreBackupConfig mConfig;

CleanupTempFilesJob(DatastoreBackupConfig config) {
	mConfig = config;
}

@Override
public Value<Void> run() throws Exception {
	ListOptions listOptions = new ListOptions.Builder()
			.setPrefix(mConfig.getBackupDirectory())
			.setRecursive(false)
			.build();
	ListResult listResult = mGcsService.list(mConfig.getGcsBucketName(), listOptions);

	while (listResult.hasNext()) {
		ListItem listItem = listResult.next();
		String filename = listItem.getName();

		// Delete temporary file
		Matcher matcher = TEMP_FILE_PATTERN.matcher(filename);
		if (matcher.find()) {
			delete(filename);
		}
		// Delete empty files
		else if (listItem.getLength() == 0) {
			delete(filename);
		}
	}

	return immediate(null);
}

private void delete(String filename) throws Exception {
	GcsFilename gcsFilename = new GcsFilename(mConfig.getGcsBucketName(), filename);
	mGcsService.delete(gcsFilename);

}
}
