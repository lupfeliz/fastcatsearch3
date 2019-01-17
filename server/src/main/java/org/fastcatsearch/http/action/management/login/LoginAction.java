package org.fastcatsearch.http.action.management.login;

import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Date;

import org.fastcatsearch.db.DBService;
import org.fastcatsearch.db.InternalDBModule.MapperSession;
import org.fastcatsearch.db.mapper.GroupAuthorityMapper;
import org.fastcatsearch.db.mapper.UserAccountMapper;
import org.fastcatsearch.db.vo.GroupAuthorityVO;
import org.fastcatsearch.db.vo.UserAccountVO;
import org.fastcatsearch.http.ActionAuthority;
import org.fastcatsearch.http.ActionAuthorityLevel;
import org.fastcatsearch.http.ActionMapping;
import org.fastcatsearch.http.SessionInfo;
import org.fastcatsearch.http.action.ActionRequest;
import org.fastcatsearch.http.action.ActionResponse;
import org.fastcatsearch.http.action.AuthAction;
import org.fastcatsearch.http.action.ServiceAction;
import org.fastcatsearch.util.ResponseWriter;

@ActionMapping("/management/login")
public class LoginAction extends ServiceAction {

	@Override
	public void doAction(ActionRequest request, ActionResponse response) throws Exception {

		writeHeader(response);
		Writer writer = response.getWriter();
		ResponseWriter resultWriter = getDefaultResponseWriter(writer);
		resultWriter.object();

		String userId = request.getParameter("id");
		String password = request.getParameter("password");

		MapperSession<UserAccountMapper> userAccountSession = null;

		MapperSession<GroupAuthorityMapper> groupAuthoritySession = null;

		try {

			userAccountSession = DBService.getInstance().getMapperSession(UserAccountMapper.class);

			groupAuthoritySession = DBService.getInstance().getMapperSession(GroupAuthorityMapper.class);

			UserAccountMapper userAccountMapper = (UserAccountMapper) userAccountSession.getMapper();
			GroupAuthorityMapper groupAuthorityMapper = (GroupAuthorityMapper) groupAuthoritySession.getMapper();

			// db에 id를 던져서 회원 정보를 가져온다.
			UserAccountVO userInfo = userAccountMapper.getEntryByUserId(userId);

			// 로그인 실패 횟수를 먼저 파악한다.
			boolean loginFailCheck = true;

			if (userInfo.loginFailCount >= 5) {

				loginFailCheck = false;
				Date loginFailTime = userInfo.loginFailDatetime;
				Date currentTime = new Date();
				long lastFailTime = (currentTime.getTime() - loginFailTime.getTime()) / 1000;
				if (lastFailTime > 180) {
					// 시간비교 후 마지막 로그인 실패 시간부터 지금까지 3분 이상 지났으면 로그인 실패 카운터 0으로 만들고 로그인 절차를 밟는다.
					userAccountMapper.resetLoginFailCheck(userId);
					loginFailCheck = true;
				} else {
					resultWriter.key("status").value("2");
				}
			}

			if (loginFailCheck) {

				// db에 passwd를 던져서 로그인 성공여부 확인.
				boolean isCorrectPassword = false;
				if (userInfo != null) {
					isCorrectPassword = userInfo.isEqualsEncryptedPassword(password);
				}

				Map<ActionAuthority, ActionAuthorityLevel> authorityMap = new HashMap<ActionAuthority, ActionAuthorityLevel>();

				if (isCorrectPassword) {

					// 로그인 성공 시 로그인 실패 회수를 초기화하고 로그인한 시간을 기록한다.
					userAccountMapper.resetLoginFailCheck(userId);
					userAccountMapper.updateSuccessLoginDate(userId);
					try {
						// db에서 내 그룹의 권한을 가져와서 authorityMap에 채워준다.
						int groupId = userInfo.groupId;
						List<GroupAuthorityVO> authorityList = groupAuthorityMapper.getEntryList(groupId);
						for (GroupAuthorityVO authority : authorityList) {
							authorityMap.put(ActionAuthority.valueOf(authority.authorityCode),
									ActionAuthorityLevel.valueOf(authority.authorityLevel));
						}
						if (authorityMap != null && authorityMap.size() != 0) {
							session.setAttribute(AuthAction.AUTH_KEY, new SessionInfo(userId, authorityMap));
						}
					} catch (Exception e) {
						userInfo = null;
						logger.error("", e);
					} finally {
					}

					resultWriter.key("status").value("0");
					resultWriter.key("name").value(userInfo.name);
					resultWriter.key("authority").object();
					for (Map.Entry<ActionAuthority, ActionAuthorityLevel> entry : authorityMap.entrySet()) {
						resultWriter.key(entry.getKey().name().toLowerCase()).value(entry.getValue().name());
					}
					resultWriter.endObject();
				} else {

					// 로그인 실패.
					userAccountMapper.updateLoginFailCheck(userId);
					userAccountMapper.updateFailLoginDate(userId);
					resultWriter.key("status").value("1");
				}
			}

			resultWriter.endObject();
		} finally {
			if (userAccountSession != null)
				try {
					userAccountSession.closeSession();
				} catch (Exception e) {
				}

			if (groupAuthoritySession != null)
				try {
					groupAuthoritySession.closeSession();
				} catch (Exception e) {
				}
		}
		resultWriter.done();
		writer.close();
	}
}
