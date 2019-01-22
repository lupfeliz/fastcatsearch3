package org.fastcatsearch.http.action.management.login;

import org.fastcatsearch.db.DBService;
import org.fastcatsearch.db.InternalDBModule;
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

import java.io.Writer;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ActionMapping("/management/password-check")
public class PasswordCheckAction extends ServiceAction {

    @Override
    public void doAction(ActionRequest request, ActionResponse response) throws Exception {

        writeHeader(response);
        Writer writer = response.getWriter();
        ResponseWriter resultWriter = getDefaultResponseWriter(writer);
        resultWriter.object();

        String userId = request.getParameter("id");
        String password = request.getParameter("password");

        InternalDBModule.MapperSession<UserAccountMapper> userAccountSession = null;

        try {

            userAccountSession = DBService.getInstance().getMapperSession(UserAccountMapper.class);

            UserAccountMapper userAccountMapper = (UserAccountMapper) userAccountSession.getMapper();

            // db에 id를 던져서 회원 정보를 가져온다.
            UserAccountVO userInfo = userAccountMapper.getEntryByUserId(userId);

            // db에 passwd를 던져서 로그인 성공여부 확인.
            boolean isCorrectPassword = false;
            if (userInfo != null) {
                isCorrectPassword = userInfo.isEqualsEncryptedPassword(password);
            }

            if (isCorrectPassword) {
                resultWriter.key("status").value("true");
            } else {
                // 로그인 실패.
                resultWriter.key("status").value("false");
            }

            resultWriter.endObject();
        } finally {
            if (userAccountSession != null)
                try {
                    userAccountSession.closeSession();
                } catch (Exception e) {
                }
        }
        resultWriter.done();
        writer.close();
    }
}
