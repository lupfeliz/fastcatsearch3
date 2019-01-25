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

package org.fastcatsearch.db;

import org.apache.ibatis.session.SqlSession;
import org.fastcatsearch.db.InternalDBModule.MapperSession;
import org.fastcatsearch.db.mapper.ExceptionHistoryMapper;
import org.fastcatsearch.db.mapper.GroupAccountMapper;
import org.fastcatsearch.db.mapper.GroupAuthorityMapper;
import org.fastcatsearch.db.mapper.IndexingHistoryMapper;
import org.fastcatsearch.db.mapper.IndexingResultMapper;
import org.fastcatsearch.db.mapper.ManagedMapper;
import org.fastcatsearch.db.mapper.NotificationConfigMapper;
import org.fastcatsearch.db.mapper.NotificationHistoryMapper;
import org.fastcatsearch.db.mapper.TaskHistoryMapper;
import org.fastcatsearch.db.mapper.UserAccountMapper;
import org.fastcatsearch.db.vo.GroupAccountVO;
import org.fastcatsearch.db.vo.GroupAuthorityVO;
import org.fastcatsearch.db.vo.UserAccountVO;
import org.fastcatsearch.env.Environment;
import org.fastcatsearch.exception.FastcatSearchException;
import org.fastcatsearch.http.ActionAuthority;
import org.fastcatsearch.http.ActionAuthorityLevel;
import org.fastcatsearch.service.ServiceManager;
import org.fastcatsearch.settings.Settings;

public class DBService extends AbstractDBService {

	protected static DBService instance;

	private static Class<?>[] mapperList = new Class<?>[] {
		ExceptionHistoryMapper.class
		, NotificationHistoryMapper.class
		, TaskHistoryMapper.class
		, IndexingHistoryMapper.class
		, IndexingResultMapper.class
		, UserAccountMapper.class
		, GroupAccountMapper.class
		, GroupAuthorityMapper.class 
		, NotificationConfigMapper.class
	};

	public static DBService getInstance() {
		return instance;
	}

	public void asSingleton() {
		instance = this;
	}

	public DBService(Environment environment, Settings settings, ServiceManager serviceManager) {
		super("db/system", DBService.mapperList, environment, settings, serviceManager);
	}

	public InternalDBModule internalDBModule() {
		return internalDBModule;
	}

	public <T> MapperSession<T> getMapperSession(Class<T> type) {
		SqlSession session = internalDBModule.openSession();
		return new MapperSession<T>(session, session.getMapper(type));

	}

	@Override
	protected boolean doStart() throws FastcatSearchException {
		if (super.doStart()) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	protected void initMapper(ManagedMapper managedMapper) throws Exception {
		if (managedMapper instanceof GroupAccountMapper) {
			GroupAccountMapper mapper = (GroupAccountMapper) managedMapper;
			mapper.putEntry(new GroupAccountVO(GroupAccountVO.ADMIN_GROUP_NAME));
		} else if (managedMapper instanceof GroupAuthorityMapper) {
			GroupAuthorityMapper mapper = (GroupAuthorityMapper) managedMapper;
			for (ActionAuthority authority : ActionAuthority.values()) {
				if (authority != ActionAuthority.NULL) {
					mapper.putEntry(new GroupAuthorityVO(1, authority.name(), ActionAuthorityLevel.WRITABLE.name()));
				}
			}
		} else if (managedMapper instanceof UserAccountMapper) {
			UserAccountMapper mapper = (UserAccountMapper) managedMapper;
			mapper.putEntry(new UserAccountVO(UserAccountVO.ADMIN_USER_NAME, UserAccountVO.ADMIN_USER_ID, settings.getString("administrator-init-password", "fastcat714%"), "", "", 1));
		}
	}

	@Override
	protected boolean doStop() throws FastcatSearchException {
		if (super.doStop()) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	protected boolean doClose() throws FastcatSearchException {
		return super.doClose();
	}

}
