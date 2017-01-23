package com.spiddekauga.appengine.pipeline.backup;

import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.appengine.tools.mapreduce.MapSettings;
import com.google.appengine.tools.pipeline.JobSetting;
import com.spiddekauga.utils.Collections;
import com.spiddekauga.utils.Time;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Backup configuration for datastore backups
 */
public class DatastoreBackupConfig implements Serializable {
private static final SimpleDateFormat DAY_FORMAT = Time.createIsoDateFormat();
private static final int SHARDS_PER_QUERY_DEFAULT = 5;
private String mGcsBucketName = AppIdentityServiceFactory.getAppIdentityService().getDefaultGcsBucketName();
private String mPrefixDirectory = "datastore_backup/";
private List<String> mBackupTables = new ArrayList<>();
private String mDayDirectory = DAY_FORMAT.format(new Date()) + "/";
private int mShardsPerQuery = SHARDS_PER_QUERY_DEFAULT;
private MapSettings mMapSettings = null;
private JobSetting[] mJobSettings = null;

String getGcsBucketName() {
	return mGcsBucketName;
}

String getPrefixDirectory() {
	return mPrefixDirectory + mDayDirectory;
}

List<String> getBackupTables() {
	return mBackupTables;
}

int getShardsPerQuery() {
	return mShardsPerQuery;
}

MapSettings getMapSettings() {
	return mMapSettings;
}

/**
 * @param additionalSettings additional (and optional) job settings
 */
public JobSetting[] getJobSettings(JobSetting... additionalSettings) {
	if (additionalSettings.length == 0) {
		return mJobSettings;
	} else {
		JobSetting[] combinedSettings = new JobSetting[additionalSettings.length + mJobSettings.length];
		System.arraycopy(mJobSettings, 0, combinedSettings, 0, mJobSettings.length);
		int offset = mJobSettings.length;
		System.arraycopy(additionalSettings, 0, combinedSettings, offset, additionalSettings.length);
		return combinedSettings;
	}
}

public static class Builder {
	private DatastoreBackupConfig mConfig = new DatastoreBackupConfig();

	/**
	 * Set the Map Settings
	 * @param mapSettings the map settings to use for the jobs
	 */
	public JobSettingsBuilder setMapSettings(MapSettings mapSettings) {
		mConfig.mMapSettings = mapSettings;
		return new JobSettingsBuilder();
	}

	public class JobSettingsBuilder {
		private JobSettingsBuilder() {
		}

		public OptionalBuilder setJobSettings(JobSetting[] jobSettings) {
			mConfig.mJobSettings = jobSettings;
			return new OptionalBuilder();
		}
	}


	public class OptionalBuilder {
		private OptionalBuilder() {
		}

		/**
		 * Set the GCS bucket name to use. If not set will use the default one
		 * @param gcsBucketName the gcs bucket name to use.
		 */
		public OptionalBuilder setGcsBucketName(String gcsBucketName) {
			mConfig.mGcsBucketName = gcsBucketName;
			return this;
		}

		/**
		 * Build the Config
		 * @return backup config
		 */
		public DatastoreBackupConfig build() {
			return mConfig;
		}

		/**
		 * Set prefix directory. Defaults to datastore_backup/
		 * @param directory prefix directory
		 */
		public OptionalBuilder setDirectory(String directory) {
			if (directory.endsWith("/")) {
				mConfig.mPrefixDirectory = directory;
			} else {
				mConfig.mPrefixDirectory = directory + "/";
			}
			return this;
		}

		/**
		 * Add a table to backup
		 * @param table a table to do a backup of
		 */
		public OptionalBuilder addBackupTable(String table) {
			mConfig.mBackupTables.add(table);
			return this;
		}

		/**
		 * Add several tables to backup
		 * @param tables several tables to do a backup of
		 */
		public OptionalBuilder addBackupTables(String[] tables) {
			Collections.addAll(tables, mConfig.mBackupTables);
			return this;
		}

		/**
		 * Add several tables to backup
		 * @param tables several tables to do a backup of
		 */
		public OptionalBuilder addBackupTables(List<String> tables) {
			mConfig.mBackupTables.addAll(tables);
			return this;
		}

		/**
		 * Set the number of shards (threads) per datastore query I.e. how many shards per table
		 * (not totally)
		 * @param shardsPerQuery number of shards (threads) per datastore query. Per table, not
		 * totally.
		 */
		public OptionalBuilder setShardsPerQuery(int shardsPerQuery) {
			mConfig.mShardsPerQuery = shardsPerQuery;
			return this;
		}
	}
}


}
