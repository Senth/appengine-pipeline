package com.spiddekauga.appengine.pipeline;

import com.google.appengine.tools.mapreduce.MapSettings;
import com.google.appengine.tools.pipeline.JobSetting;
import com.spiddekauga.utils.Collections;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for all datastore jobs
 */
public class DatastoreJobConfig implements Serializable {
private static final int SHARDS_PER_QUERY_DEFAULT = 5;
private int mShardsPerQuery = SHARDS_PER_QUERY_DEFAULT;
private MapSettings mMapSettings = null;
private JobSetting[] mJobSettings = null;
private List<String> mTables = new ArrayList<>();

private DatastoreJobConfig() {
}

public int getShardsPerQuery() {
	return mShardsPerQuery;
}

public MapSettings getMapSettings() {
	return mMapSettings;
}

public JobSetting[] getJobSettings() {
	return mJobSettings;
}

public List<String> getTables() {
	return mTables;
}

public static class Builder {
	private DatastoreJobConfig mConfig = new DatastoreJobConfig();

	public Builder() {
	}

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
		 * Add a table to query
		 * @param table a table to query
		 */
		public OptionalBuilder addTable(String table) {
			mConfig.mTables.add(table);
			return this;
		}

		/**
		 * Add several tables to query
		 * @param tables several tables to query
		 */
		public OptionalBuilder addTables(String[] tables) {
			Collections.addAll(tables, mConfig.mTables);
			return this;
		}

		/**
		 * Add several tables to query
		 * @param tables several tables to query
		 */
		public OptionalBuilder addTables(List<String> tables) {
			mConfig.mTables.addAll(tables);
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

		/**
		 * Build the Config
		 * @return backup config
		 */
		public DatastoreJobConfig build() {
			return mConfig;
		}
	}
}
}
