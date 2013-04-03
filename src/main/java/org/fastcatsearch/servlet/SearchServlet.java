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

package org.fastcatsearch.servlet;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.fastcatsearch.control.JobController;
import org.fastcatsearch.control.JobResult;
import org.fastcatsearch.ir.config.FieldSetting;
import org.fastcatsearch.ir.config.IRSettings;
import org.fastcatsearch.ir.config.Schema;
import org.fastcatsearch.ir.config.SettingException;
import org.fastcatsearch.ir.field.ScoreField;
import org.fastcatsearch.ir.group.GroupEntry;
import org.fastcatsearch.ir.group.GroupResult;
import org.fastcatsearch.ir.group.GroupResults;
import org.fastcatsearch.ir.io.AsciiCharTrie;
import org.fastcatsearch.ir.query.Result;
import org.fastcatsearch.ir.query.Row;
import org.fastcatsearch.ir.util.Formatter;
import org.fastcatsearch.job.SearchJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchServlet extends HttpServlet {
	
	private static final long serialVersionUID = 963640595944747847L;
	private static Logger logger = LoggerFactory.getLogger(SearchServlet.class);
	private static Logger searchLogger = LoggerFactory.getLogger("SEARCH_LOG");
	private static AtomicLong taskSeq = new AtomicLong();
	public static final int JSON_TYPE = 0;
	public static final int XML_TYPE = 1;
	public static final int JSONP_TYPE = 2;
	public static final int IS_ALIVE = 3;
	
	private int RESULT_TYPE = JSON_TYPE;
	
	public void init(){
		String type = getServletConfig().getInitParameter("result_format");
		if(type != null){
			if(type.equalsIgnoreCase("json")){
				RESULT_TYPE = JSON_TYPE;
			}else if(type.equalsIgnoreCase("xml")){
				RESULT_TYPE = XML_TYPE;
			}else if(type.equalsIgnoreCase("jsonp")){
				RESULT_TYPE = JSONP_TYPE;
			}
		}
	}
	
	public SearchServlet() {
		this(JSON_TYPE);
	}
	
    public SearchServlet(int resultType){
    	RESULT_TYPE = resultType;
    }
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	@SuppressWarnings("rawtypes")
		Enumeration enumeration = request.getParameterNames();
    	String timeoutStr = request.getParameter("timeout");
    	String isAdmin = request.getParameter("admin");
    	String collectionName = request.getParameter("cn");
    	String requestCharset = request.getParameter("requestCharset");
    	String responseCharset = request.getParameter("responseCharset");
    	String userData = request.getParameter("ud");
    	StringBuffer sb = new StringBuffer();
    	while(enumeration.hasMoreElements()){
    		String key = (String) enumeration.nextElement();
    		String value = request.getParameter(key);
    		sb.append(key);
    		sb.append("=");
    		sb.append(value);
    		sb.append("&");
    	}
    	doReal(sb.toString(), timeoutStr, isAdmin, collectionName, userData, request, response, requestCharset, responseCharset);
    }
    
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	String queryString = request.getQueryString();
    	String timeoutStr = request.getParameter("timeout");
    	String isAdmin = request.getParameter("admin");
    	String collectionName = request.getParameter("cn");
    	String requestCharset = request.getParameter("requestCharset");
    	String responseCharset = request.getParameter("responseCharset");
    	String userData = request.getParameter("ud");
    	doReal(queryString, timeoutStr, isAdmin, collectionName, userData, request, response, requestCharset, responseCharset);
    	
    }
    
    private void doReal(String queryString, String timeoutStr, String isAdmin, String collectionName, String userData, HttpServletRequest request, HttpServletResponse response, String requestCharset, String responseCharset) throws ServletException, IOException {
    	if(requestCharset == null)
    		requestCharset = "UTF-8";
    	
    	if(responseCharset == null)
    		responseCharset = "UTF-8";
    	
//    	logger.debug("requestCharset = "+requestCharset);
//    	logger.debug("responseCharset = "+responseCharset);
    	if(queryString != null){
    		queryString = URLDecoder.decode(queryString, requestCharset);
    		if(queryString.endsWith("&")) { 
    			queryString = queryString.substring(0,queryString.length()-1); 
    		}
    	}
    	logger.debug("queryString = "+queryString);
    	
    	//TODO 디폴트 시간을 셋팅으로 빼자.
    	int timeout = 5;
    	if(timeoutStr != null)
    		timeout = Integer.parseInt(timeoutStr);
    	logger.debug("timeout = "+timeout+" s");
    	
    	long seq = taskSeq.incrementAndGet();
		searchLogger.info(seq+", "+queryString);
		
		
		response.setCharacterEncoding(responseCharset);
    	response.setStatus(HttpServletResponse.SC_OK);
    	
    	PrintWriter w = response.getWriter();
    	BufferedWriter writer = new BufferedWriter(w);
    	
    	if(RESULT_TYPE == JSON_TYPE){
    		response.setContentType("application/json; charset="+responseCharset);
    	}else if(RESULT_TYPE == XML_TYPE){
    		response.setContentType("text/xml; charset="+responseCharset);
    	}else if(RESULT_TYPE == JSONP_TYPE){
    		response.setContentType("application/json; charset="+responseCharset);
    	}else if(RESULT_TYPE == IS_ALIVE){
    		response.setContentType("text/html; charset="+responseCharset);
    		writer.write("FastCat/OK\n<br/>" + new Date());
    		writer.close();
    		return;
    	}
    	
    	
    	if(RESULT_TYPE == JSONP_TYPE) {
    		String callback = request.getParameter("jsoncallback");
    		writer.write(callback+"(");
    	}
    	
    	long searchTime = 0;
    	long st = System.currentTimeMillis();
    	
    	SearchJob job = new SearchJob();
    	job.setArgs(new String[]{queryString});
    	
    	Result result = null;
    	
		JobResult jobResult = JobController.getInstance().offer(job);
		Object obj = jobResult.poll(timeout);
		searchTime = (System.currentTimeMillis() - st);
		if(jobResult.isSuccess()){
			result = (Result)obj;
		}else{
			String errorMsg = (String)obj;
			searchLogger.info(seq+", -1, "+errorMsg);
			
			if(RESULT_TYPE == JSON_TYPE){
				if(errorMsg != null){
					errorMsg = Formatter.escapeJSon(errorMsg);
				}
				writer.write("{");
				writer.newLine();
	    		writer.write("\t\"status\": \"1\",");
	    		writer.newLine();
	    		writer.write("\t\"time\": \""+Formatter.getFormatTime(searchTime)+"\",");
	    		writer.newLine();
	    		writer.write("\t\"total_count\": \"0\",");
	    		writer.newLine();
	    		writer.write("\t\"error_msg\": \""+errorMsg+"\"");
	    		writer.newLine();
	    		writer.write("}");
			}else if(RESULT_TYPE == XML_TYPE){
				if(errorMsg != null){
					errorMsg = Formatter.escapeXml(errorMsg);
				}
				writer.write("<fastcat>");
				writer.newLine();
	    		writer.write("\t<status>1</status>");
	    		writer.newLine();
	    		writer.write("\t<total_count>0</total_count>");
	    		writer.newLine();
	    		writer.write("\t<time>"+Formatter.getFormatTime(searchTime)+"</time>");
	    		writer.newLine();
	    		writer.write("\t<error_msg>"+errorMsg+"</error_msg>");
	    		writer.newLine();
	    		writer.write("</fastcat>");
			}
			
			writer.close();
    		return;
		}
		
    	//SUCCESS
		String logStr = searchTime+", "+result.getCount()+", "+result.getTotalCount()+", "+result.getFieldCount();
		if(result.getGroupResult() != null){
			String grStr = ", [";
			GroupResults gr = result.getGroupResult();
			for (int i = 0; i < gr.groupSize(); i++) {
				if(i > 0)
					grStr += ", ";
				grStr += gr.getGroupResult(i).size();
			}
			grStr += "]";
			logStr += grStr;
		}
		searchLogger.info(seq+", "+logStr);
		
		if(RESULT_TYPE == JSON_TYPE || RESULT_TYPE == JSONP_TYPE){
			//JSON
			int fieldCount = result.getFieldCount();
			writer.write("{");
			writer.newLine();
			writer.write("\t\"status\": \"0\",");
			writer.newLine();
			writer.write("\t\"time\": \""+Formatter.getFormatTime(searchTime)+"\",");
			writer.newLine();
			writer.write("\t\"total_count\": \""+result.getTotalCount()+"\",");
			writer.newLine();
			writer.write("\t\"count\": \""+result.getCount()+"\",");
			writer.newLine();
			writer.write("\t\"field_count\": \""+fieldCount+"\",");
			writer.newLine();
			writer.write("\t\"fieldname_list\": [");
			writer.newLine();
			writer.write("\t\t");
			
			String[] fieldNames = result.getFieldNameList();
			
			if(result.getCount() == 0){
				writer.write("{\"name\": \"_no_\"}");
			}else{
	    		writer.write("{\"name\": \"_no_\"}, ");
	    		for (int i = 0; i < fieldNames.length; i++) {
	    			writer.write("{\"name\": ");
	    			writer.write("\""+fieldNames[i]+"\"}");
	    			if(i < fieldNames.length - 1)
						writer.write(",");
				}
	    		writer.write("");
			}
	    	
			writer.write("\t],");
			
			if(isAdmin != null && isAdmin.equalsIgnoreCase("true")){
				if(result.getCount() == 0){
					writer.write("\t\"colmodel_list\": [");
	        		writer.write("\t\t{\"name\": \"_no_\", \"index\": \"_no_\", \"width\": \"100%\", \"sorttype\": \"int\", \"align\": \"center\"}");
	        		writer.write("\t],");
				}else{
	    			writer.write("\t\"colmodel_list\": [");
	        		writer.write("\t\t");
	        		AsciiCharTrie fieldnames = null;
	        		List<FieldSetting> fieldSettingList = null;
	        		try {
	        			Schema schema = IRSettings.getSchema(collectionName, false);
	        			fieldnames = schema.fieldnames;
	        			fieldSettingList = schema.getFieldSettingList();
					} catch (SettingException e) {
						
					}
					
					
					//write _no_
					if(fieldNames.length > 0){
						writer.write("{\"name\": \"_no_\", \"index\": \"_no_\", \"width\": \"20\", \"sorttype\": \"int\", \"align\": \"center\"}");
					}
	        		for (int i = 0; i < fieldNames.length; i++) {
	        			int idx = fieldnames.get(fieldNames[i]);
	        			if(idx < 0){
	        				if(fieldNames[i].equalsIgnoreCase(ScoreField.fieldName)){
	        					writer.write(",");
	        					writer.write("\t\t{\"name\": \"");
	                			writer.write(fieldNames[i]);
	                			writer.write("\", \"index\": \"");
	                			writer.write(fieldNames[i]);
	                			writer.write("\", \"width\": ");
	        					writer.write("\"20\", \"sorttype\": \"int\", \"align\": \"right\"}");
	        				}else{
	        					//Unknown Field
	        					writer.write(",");
	        					writer.write("\t\t{\"name\": \"");
	                			writer.write(fieldNames[i]);
	                			writer.write("\", \"index\": \"");
	                			writer.write(fieldNames[i]);
	                			writer.write("\", \"width\": ");
	        					writer.write("\"20\"}");
	        				}
	        			}else{
	        				writer.write(",");
	            			writer.write("\t\t{\"name\": \"");
	            			writer.write(fieldNames[i]);
	            			writer.write("\", \"index\": \"");
	            			writer.write(fieldNames[i]);
	            			writer.write("\", \"width\": ");
	        			
	            			FieldSetting fs = fieldSettingList.get(idx);
	            			if(fs.type == FieldSetting.Type.Int || fs.type == FieldSetting.Type.Long || fs.type == FieldSetting.Type.Float || fs.type == FieldSetting.Type.Double || fs.type == FieldSetting.Type.DateTime){
	            				writer.write("\"20\", \"sorttype\": \"int\", \"align\": \"right\"}");
	            			}else{
	            				if(fs.size > 0 && fs.size < 50){
	            					writer.write("\"");
	            					writer.write((fs.size * 2)+"");
	            					writer.write("\", ");
	            				}else{
	            					writer.write("\"100\", ");
	            				}
	            				writer.write("\"sorttype\": \"text\"}");
	            			}
	            			
	        			}
	        			
	//            			if(i < fieldNames.length - 1)
	//            				writer.write(",");
	        			
					}
	        		writer.write("");
	        		writer.write("\t],");
				}
			}
			
			
			writer.write("\t\"result\":");
			writer.write("\t["); //array
			//data
			Row[] rows = result.getData();
			int start = result.getMetadata().start();
			
			if(rows.length == 0){
				writer.write("\t\t{\"_no_\": \"No result found!\"}");
				writer.write("\t]");
			}else{
	    		for (int i = 0; i < rows.length; i++) {
	    			writer.write("\t\t{");
					Row row = rows[i];
					writer.write("\t\t\"_no_\": \""+(start + i)+"\",");
					for(int k = 0; k < fieldCount; k++) {
						char[] f = row.get(k);
						String fdata = new String(f).trim();
						//For Json validity
						//1. replace single back-slash quote with double back-slash
						//2. replace double quote with character '\"' 
						//4. replace linefeed with character '\n' 
	//							fdata = fdata.replaceAll("\r\n", "\\\\n").replaceAll("\n", "").replaceAll("\r", "").replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\"").replaceAll("\t", "\\\\t");
						fdata = Formatter.escapeJSon(fdata);
						writer.write("\t\t\""+fieldNames[k]+"\": \""+fdata+"\"");
	//						writer.write("\t\t\""+fieldNames[k]+"\": \""+URLEncoder.encode(fdata, "utf-8")+"\"");
						if(k < fieldCount - 1)
							writer.write(",");
						else
							writer.write("");
					}
					writer.write("\t\t}");
					if(i < rows.length - 1)
						writer.write(",");
					else
						writer.write("");
	    		}
	    		
	    		writer.write("\t],");
	    		
	    		//group
	    		writer.write("\t\"group_result\":");
	    		GroupResults groupResults = result.getGroupResult();
	    		if(groupResults == null){
	    			writer.write(" \"null\"");
	    		}else{
	    			writer.write("");
	        		writer.write("\t[");
	        		for (int i = 0; i < groupResults.groupSize(); i++) {
	        			writer.write("\t\t[");
						GroupResult groupResult = groupResults.getGroupResult(i);
						int size = groupResult.size();
						for (int k = 0; k < size; k++) {
							GroupEntry e = groupResult.getEntry(k);
							String keyData = e.key.getKeyString();
							String functionName = groupResult.functionName();
							keyData = Formatter.escapeJSon(keyData);
							
							writer.write("\t\t{\"_no_\": \"");
							writer.write((k+1)+"");
							writer.write("\", \"key\": \"");
							writer.write(keyData);
							writer.write("\", \"freq\": \"");
							writer.write(e.count()+"");
							if(groupResult.hasAggregationData()){
								String r = e.getGroupingObjectResultString();
								if(r == null){
									r = "";
								}
								writer.write("\", \""+functionName+"\": \"");
								writer.write(r);
							}
							writer.write("\"}");
							if(k < size - 1)
								writer.write(",");
							else
								writer.write("");
							
						}
						writer.write("\t\t]");
						if(i < groupResults.groupSize() - 1)
							writer.write(",");
						else
							writer.write("");
						
					}
	        		
	        		writer.write("\t]");
	    		}//for
			}//if else
			writer.write("}");
		}else if(RESULT_TYPE == XML_TYPE){
			//XML
			//this does not support admin test, have no column meta data
			
			int fieldCount = result.getFieldCount();
			writer.write("<fastcat>");
			writer.newLine();
			writer.write("\t<status>0</status>");
			writer.newLine();
			writer.write("\t<time>");
			writer.write(Formatter.getFormatTime(searchTime));
			writer.write("</time>");
			writer.newLine();
			writer.write("\t<total_count>");
			writer.write(result.getTotalCount()+"");
			writer.write("</total_count>");
			writer.newLine();
			writer.write("\t<count>");
			writer.write(result.getCount()+"");
			writer.write("</count>");
			writer.newLine();
			writer.write("\t<field_count>");
			writer.write(fieldCount+"");
			writer.write("</field_count>");
			writer.newLine();
			
			String[] fieldNames = result.getFieldNameList();
			
			if(result.getCount() == 0){
				writer.write("\t<fieldname_list>");
				writer.newLine();
				writer.write("\t\t<name>_no_</name>");
				writer.newLine();
				writer.write("\t</fieldname_list>");
				writer.newLine();
			}else{
				writer.write("\t<fieldname_list>");
				writer.newLine();
	    		writer.write("\t\t<name>_no_</name>");
	    		writer.newLine();
				writer.write("\t</fieldname_list>");
				writer.newLine();
	    		for (int i = 0; i < fieldNames.length; i++) {
	    			writer.write("\t<fieldname_list>");
					writer.newLine();
	    			writer.write("\t\t<name>");
	    			writer.write(fieldNames[i]);
	    			writer.write("</name>");
	    			writer.newLine();
	    			writer.write("\t</fieldname_list>");
					writer.newLine();
				}
			}
	    	
			
			//data
			Row[] rows = result.getData();
			int start = result.getMetadata().start();
			
			if(rows.length == 0){
				writer.write("\t<result>");
				writer.newLine();
				writer.write("\t\t<_no_>No result found!</_no_>");
				writer.newLine();
				writer.write("\t</result>");
				writer.newLine();
			}else{
	    		for (int i = 0; i < rows.length; i++) {
					Row row = rows[i];
					writer.write("\t<result>");
					writer.newLine();
					writer.write("\t\t<_no_>");
					writer.write((start + i)+"");
					writer.write("</_no_>");
					writer.newLine();
					for(int k = 0; k < fieldCount; k++) {
						char[] f = row.get(k);
						String fdata = new String(f);
						fdata = Formatter.escapeXml(fdata);
						
						writer.write("\t\t<");
						writer.write(fieldNames[k]);
						writer.write(">");
						writer.write(fdata);
						writer.write("</");
						writer.write(fieldNames[k]);
						writer.write(">");
						writer.newLine();
					}
					writer.write("\t</result>");
					writer.newLine();
	    		}
	    		
	    		//group
	    		GroupResults groupResultList = result.getGroupResult();
	    		if(groupResultList == null){
	    			writer.write("\t<group_result />");
	    			writer.newLine();
	    		}else{
	    			writer.write("\t<group_result>");
	    			writer.newLine();
	        		for (int i = 0; i < groupResultList.groupSize(); i++) {
	        			writer.write("\t\t<group_list>");
	        			writer.newLine();
						GroupResult groupResult = groupResultList.getGroupResult(i);
						int size = groupResult.size();
						for (int k = 0; k < size; k++) {
							writer.write("\t\t\t<group_item>");
							writer.newLine();
							GroupEntry e = groupResult.getEntry(k);
							String keyData = e.key.getKeyString();
							keyData = Formatter.escapeXml(keyData);
							String functionName = groupResult.functionName();
							
							writer.write("\t\t\t\t<_no_>");
							writer.write((k+1)+"");
							writer.write("</_no_>");
							writer.newLine();
							writer.write("\t\t\t\t<key>");
							writer.write(keyData);
							writer.write("</key>");
							writer.newLine();
							writer.write("\t\t\t\t<freq>");
							writer.write(e.count()+"");
							writer.write("</freq>");
							writer.newLine();
							if(groupResult.hasAggregationData()){
								String r = e.getGroupingObjectResultString();
								if(r == null){
									r = "";
								}
								writer.write("\t\t\t\t<"+functionName+">");
								writer.write(r);
								writer.write("</"+functionName+">");
								writer.newLine();
							}
							writer.write("\t\t\t</group_item>");
							writer.newLine();
							
						}
						writer.write("\t\t</group_list>");
						writer.newLine();
					}
	        		writer.write("\t</group_result>");
	    			writer.newLine();
	    			
	    		}//for
			}//if else
			writer.write("</fastcat>");
		}
		
    	if(RESULT_TYPE == JSONP_TYPE) {
    		writer.write(");");
    	}

    	writer.close();
    }
}
