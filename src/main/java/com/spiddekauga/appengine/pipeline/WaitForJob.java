package com.spiddekauga.appengine.pipeline;

import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Value;

/**
 * A job that waits for other jobs. So that we can return a FutureValue&lt;Void&gt; instead of
 * returning a list of MapReduce or other values that won't be used
 */
public class WaitForJob extends Job1<Void, Object> {
@Override
public Value<Void> run(Object object) throws Exception {
	return immediate(null);
}

@Override
public String getJobDisplayName() {
	return "Waiting for jobs";
}
}
