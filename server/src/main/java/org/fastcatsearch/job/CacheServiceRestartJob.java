/*
 * Copyright (c) 2013 Websquared, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v2.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 * 
 * Contributors:
 *     swsong - initial API and implementation
 */

package org.fastcatsearch.job;


import org.fastcatsearch.ir.IRService;
import org.fastcatsearch.service.ServiceManager;
import org.fastcatsearch.exception.FastcatSearchException;

public class CacheServiceRestartJob extends Job{
	private int delay;
	
	public CacheServiceRestartJob(){ }
	
	public CacheServiceRestartJob(int delay){ 
		this.delay = delay;
	}
	
	@Override
	public JobResult doRun() {
		try {
			Thread.sleep(delay);
		} catch (InterruptedException e) { }
		
		IRService irService = ServiceManager.getInstance().getService(IRService.class);
		
		boolean result = irService.searchCache().unload()
		&& irService.groupingCache().unload()
		&& irService.documentCache().unload()
		&& irService.searchCache().load()
		&& irService.groupingCache().load()
		&& irService.documentCache().load();
		
		return new JobResult(result);
	}

}