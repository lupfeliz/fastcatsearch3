package org.fastcatsearch.http.action.service;

import org.fastcatsearch.job.Job;
import org.fastcatsearch.job.cluster.ClusterSearchJob;
import org.fastcatsearch.query.QueryMap;

public class ClusterSearchAction extends AbstractSearchAction {

	public ClusterSearchAction(String type) {
		super(type);
	}

	@Override
	protected Job createSearchJob(QueryMap queryMap) {
		ClusterSearchJob job = new ClusterSearchJob();
		job.setArgs(queryMap);
		return job;
	}

}