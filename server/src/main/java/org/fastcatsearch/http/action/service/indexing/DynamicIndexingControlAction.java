package org.fastcatsearch.http.action.service.indexing;

import org.fastcatsearch.cluster.Node;
import org.fastcatsearch.cluster.NodeService;
import org.fastcatsearch.control.ResultFuture;
import org.fastcatsearch.http.ActionMapping;
import org.fastcatsearch.http.ActionMethod;
import org.fastcatsearch.http.action.ActionException;
import org.fastcatsearch.http.action.ActionRequest;
import org.fastcatsearch.http.action.ActionResponse;
import org.fastcatsearch.http.action.ServiceAction;
import org.fastcatsearch.ir.IRService;
import org.fastcatsearch.ir.config.CollectionContext;
import org.fastcatsearch.ir.config.IndexingScheduleConfig;
import org.fastcatsearch.ir.search.CollectionHandler;
import org.fastcatsearch.job.indexing.UpdateDynamicIndexingScheduleJob;
import org.fastcatsearch.job.indexing.UpdateIndexingScheduleJob;
import org.fastcatsearch.service.ServiceManager;
import org.fastcatsearch.settings.SettingFileNames;
import org.fastcatsearch.util.JAXBConfigs;
import org.fastcatsearch.util.ResponseWriter;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
/**
 * 1. 증분색인 스케쥴을 시작/정지한다.(셋팅까지 기록)
 * 2. 동적(REST API) 색인을 시작/정지한다
 * @See IndexingScheduleAction
 * */
@ActionMapping(value = "/service/index/working", method = { ActionMethod.POST })
public class DynamicIndexingControlAction extends ServiceAction {

    private static final String FLAG_ON = "on";
    private static final String FLAG_OFF = "off";

	@Override
	public void doAction(ActionRequest request, ActionResponse response) throws Exception {

        String collectionId = request.getParameter("collectionId");
        String flag = request.getParameter("flag");

        NodeService nodeService = ServiceManager.getInstance().getService(NodeService.class);
        IRService irService = ServiceManager.getInstance().getService(IRService.class);
        CollectionHandler collectionHandler = irService.collectionHandler(collectionId);
        if (collectionHandler != null) {
            UpdateIndexingScheduleJob job = new UpdateIndexingScheduleJob(collectionId, "add", flag);
            ResultFuture future = nodeService.sendRequest(nodeService.getMasterNode(), job);
            //증분색인 스케쥴
            Object r = future.take();
            boolean result1 = false;
            if(r instanceof Boolean) {
                result1 = (Boolean) r;
            }

            UpdateDynamicIndexingScheduleJob job2 = new UpdateDynamicIndexingScheduleJob(collectionId, flag);
            String indexNodeId = collectionHandler.collectionContext().collectionConfig().getIndexNode();
            Node indexNode = nodeService.getNodeById(indexNodeId);
            ResultFuture future2 = nodeService.sendRequest(indexNode, job2);
            //동적색인 스케쥴
            r = future2.take();
            boolean result2 = false;
            if(r instanceof Boolean) {
                result2 = (Boolean) r;
            }

            writeHeader(response);
            response.setStatus(HttpResponseStatus.OK);
            ResponseWriter resultWriter = getDefaultResponseWriter(response.getWriter());
            resultWriter.object().key("collectionId").value(collectionId).key("incrementIndexing").value(result1 ? FLAG_ON : FLAG_OFF)
                    .key("dynamicIndexing").value(result2 ? FLAG_ON : FLAG_OFF).endObject();
            resultWriter.done();
        } else {
            //컬렉션 없음.
            response.setStatus(HttpResponseStatus.NOT_FOUND);
            writeHeader(response);
            ResponseWriter resultWriter = getDefaultResponseWriter(response.getWriter());
            resultWriter.object().key(collectionId).value(false).endObject();
            resultWriter.done();
        }
	}

}
