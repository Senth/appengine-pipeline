package com.spiddekauga.appengine.pipeline;

import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Waits for and combines the result from multiple jobs that returns a list. Since {@link
 * com.google.appengine.tools.pipeline.FutureList} can't combine values.
 * @param <InnerType> Will return List<InnerType>
 * <p>
 * Example call
 * <pre>
 * <code>List&lt;FutureValue&lt;List&lt;InnerType&gt;&gt;&gt; separateLists;<br/>
 * FutureValue&lt;List&lt;InnerType&gt;&gt combinedList =<br/>
 * 	futureCall(new CombineListResults&lt;InnerType&gt;(), futureList(separateLists));
 * </code>
 * </pre>
 */
public class CombineListResults<InnerType> extends Job1<List<InnerType>, List<List<InnerType>>> {
private static final long serialVersionUID = -7143355720759335535L;

@Override
public Value<List<InnerType>> run(List<List<InnerType>> separateLists) throws Exception {
	List<InnerType> combinedList = new ArrayList<>();

	for (List<InnerType> innerList : separateLists) {
		combinedList.addAll(innerList);
	}

	return immediate(combinedList);
}
}
