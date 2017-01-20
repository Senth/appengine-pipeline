package com.spiddekauga.appengine.pipeline;

import com.google.api.services.bigquery.model.JobReference;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.impl.util.SerializableValue;

import java.io.Serializable;

/**
 * Result of the bigquery load files pipeline job.
 */
@SuppressWarnings("javadoc")
public class BigQueryLoadJobReference implements Serializable {
private static final long serialVersionUID = -5045977572520245900L;
private final String mStatus;
private final SerializableValue<JobReference> mJobReference;

public BigQueryLoadJobReference(String status, JobReference jobReference) {
	mStatus = status;
	mJobReference = SerializableValue.of(Marshallers.getGenericJsonMarshaller(JobReference.class), jobReference);
}

public String getStatus() {
	return mStatus;
}

public JobReference getJobReference() {
	return mJobReference.getValue();
}
}
