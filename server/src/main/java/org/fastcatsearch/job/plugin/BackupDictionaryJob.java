package org.fastcatsearch.job.plugin;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.fastcatsearch.exception.FastcatSearchException;
import org.fastcatsearch.http.action.ActionRequest;
import org.fastcatsearch.http.action.ActionResponse;
import org.fastcatsearch.http.action.management.dictionary.GetDictionaryWordListAction;
import org.fastcatsearch.job.Job;
import org.fastcatsearch.plugin.Plugin;
import org.fastcatsearch.plugin.PluginService;
import org.fastcatsearch.plugin.analysis.AnalysisPlugin;
import org.fastcatsearch.plugin.analysis.AnalysisPluginSetting;
import org.fastcatsearch.plugin.analysis.AnalysisPluginSetting.DictionarySetting;
import org.fastcatsearch.service.ServiceManager;

public class BackupDictionaryJob extends Job {

	private static final long serialVersionUID = 4891838956215820136L;
	
	//FIXME:HardCoding..
	private static final int BACKUP_SIZE = 3;

	@Override
	public JobResult doRun() throws FastcatSearchException {
		
		String pluginId = (String)getArgs();
		
		PluginService pluginService = ServiceManager.getInstance().getService(PluginService.class);
		
		Plugin plugin = pluginService.getPlugin(pluginId);
		
		@SuppressWarnings("rawtypes")
		AnalysisPlugin analysisPlugin = (AnalysisPlugin) plugin;
		
		File baseDir = analysisPlugin.getDictionaryDirectory();
		File backupDir = new File(baseDir, "backup");
		
		if(!backupDir.exists()) {
			try {
				FileUtils.forceMkdir(backupDir);
			} catch (IOException e) { }
		}
		
		File tmpDir = null;
		
		try {
		
			tmpDir = File.createTempFile("dict_backup", "");
			tmpDir.delete();
			tmpDir.mkdir();
			
			AnalysisPluginSetting analysisPluginSetting = (AnalysisPluginSetting) plugin.getPluginSetting();
			List<DictionarySetting> dictionaryList = analysisPluginSetting.getDictionarySettingList();
			
			if(dictionaryList != null){
				for(DictionarySetting dictionary : dictionaryList){
					String dictionaryId = dictionary.getId();
					logger.debug("backup dictionary {}.{}", new Object[] {pluginId, dictionaryId});
					downloadFile(tmpDir, pluginId, dictionaryId);
				}
			}
			
			for(int inx=BACKUP_SIZE;inx >=1; inx--) {
				File oldBackup = new File(backupDir, String.valueOf(inx));
				if(inx==BACKUP_SIZE) {
					if(oldBackup.exists()) {
						//오래된 백업은 지운다.
						FileUtils.forceDelete(oldBackup);
					}
				} else {
					if(oldBackup.exists()) {
						File toBackup = new File(backupDir, String.valueOf(inx+1));
						FileUtils.moveDirectory(oldBackup, toBackup);
					}
				}
				if(inx==1) {
					FileUtils.moveDirectory(tmpDir, oldBackup);
				}
			}
			
			return new JobResult("OK");
		} catch (Exception e) {
			logger.error("",e);
			if(tmpDir!=null) try {
				FileUtils.forceDelete(tmpDir);
			} catch (IOException e2) { }
		}
		
		return new JobResult(null);
	}
	
	public void downloadFile(File backupDir, String pluginId, String dictionaryId) {
		
		File tmpFile = null;
		Writer writer = null;
		try {
			
			tmpFile = new File(backupDir, dictionaryId+".txt");
			writer = new FileWriter(tmpFile);
			
			final Map<String,String>parameterMap = new HashMap<String,String>();
			parameterMap.put("pluginId", pluginId);
			parameterMap.put("dictionaryId", dictionaryId);
			parameterMap.put("search", null);
			parameterMap.put("start", "1");
			parameterMap.put("length", "-1");
			parameterMap.put("searchColumns", null);
			parameterMap.put("sortAsc", "true");
			
			ActionRequest request = new ActionRequest(pluginId, null) {
				@Override public String getParameter(String key) {
					return parameterMap.get(key);
				}
			};
			
			ActionResponse response = new ActionResponse() {
				private Writer writer;
				public ActionResponse setWriter(Writer writer) {
					this.writer = writer;
					return this;
				}
				@Override public Writer getWriter() {
					return writer;
				}
			}.setWriter(writer);
			
			GetDictionaryWordListAction action = new GetDictionaryWordListAction() {
				public GetDictionaryWordListAction getInstance() {
					this.resultType=Type.json;
					return this;
				}
			}.getInstance();
		
			action.doAuthAction(request, response);
			
			logger.debug("done");
			
		} catch (Exception e) {
			logger.error("",e);
		} finally {
			try { 
				writer.close(); 
			} catch (IOException e) { }
		}
	}
}