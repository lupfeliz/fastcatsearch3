package org.fastcatsearch.ir.field;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.fastcatsearch.ir.io.DataInput;
import org.fastcatsearch.ir.io.DataOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Field implements Cloneable {
	protected static Logger logger = LoggerFactory.getLogger(Field.class);
	protected static final String DEFAULT_MULTI_VALUE_DELIMITER = "\n";

	protected String id;
	protected int size;
	protected boolean store;
	protected boolean removeTag;
	protected boolean multiValue;
	protected String rawString;
	protected Object fieldsData;

	public Field(String id) {
		this.id = id;
	}

	public Field(String id, String data) {
		this.id = id;
		this.rawString = data;
	}

	public Field(String id, int size) {
		this.id = id;
		this.size = size;
	}

	public Field(String id, String data, int size) {
		this.id = id;
		this.rawString = data;
		this.size = size;
	}

	public String rawString() {
		return rawString;
	}

	public Field parseIndexable() throws FieldDataParseException {
		parseIndexable(null);
		return this;
	}

	public void parseIndexable(String multiValueDelimiter) throws FieldDataParseException {
		if (rawString != null) {
			if (multiValue) {
				if(multiValueDelimiter == null){
					multiValueDelimiter = DEFAULT_MULTI_VALUE_DELIMITER;
				}
				// 2018-12-12 로즈시스템즈 전제현
				// 파이프를 구분자로 사용 시에는 [|] 형태로 정규식에 사용되어야 함
				// 이에 대한 대비책 필요
//				if (multiValueDelimiter.equals("|")) {
//					multiValueDelimiter = "[" + multiValueDelimiter + "]";
//				}
//				StringTokenizer tokenizer = new StringTokenizer(rawString.trim(), multiValueDelimiter);
//				while (tokenizer.hasMoreElements()) {
//					addValue(tokenizer.nextToken().trim());
//				}
				int index = 0;
				Matcher matcher = Pattern.compile(multiValueDelimiter).matcher(rawString);
				while(matcher.find()) {
//					System.out.println(index + " : "+ matcher.start());
					int chkStart = matcher.start();
//					if(index != matcher.start()){
					if(index != chkStart){
						addValue(rawString.substring(index, matcher.start()));
					}
					index = matcher.end();
		        }
		        if (index == 0){
		        	addValue(rawString);
		        }else{
//		        	System.out.println(index + " ::: "+ rawString.length());
		        	if(index != rawString.length()){
		        		addValue(rawString.substring(index, rawString.length()));
		        	}
		        }
					
			} else {
				fieldsData = parseData(rawString);
			}
		}
	}

	public String toString() {
		if(rawString != null){
			return rawString;
		} else if (fieldsData != null) {
			return fieldsData.toString();
		}else{
			return "";
		}
	}

	public void addValue(String value) throws FieldDataParseException {
		if (!multiValue) {
			throw new UnsupportedOperationException("multivalue가 아닌 필드에는 값을 추가할 수 없습니다.");
		}

		if(fieldsData == null){
			fieldsData = new ArrayList<Object>();
		}
		Object v = parseData(value);
		((List<Object>) fieldsData).add(v);
//		logger.debug(">>> {}", v);

	}

	public Object getValue() {
		return fieldsData;
	}
	
	public int getMultiValueCount() {
		if (!multiValue) {
			throw new UnsupportedOperationException("multivalue가 아닌 필드는 지원하지 않습니다.");
		}
		if(fieldsData == null){
			return 0;
		}
		return ((List<Object>) fieldsData).size();
	}
	public List<Object> getMultiValues() {
		if (!multiValue) {
			throw new UnsupportedOperationException("multivalue가 아닌 필드는 지원하지 않습니다.");
		}
		
		return (List<Object>) fieldsData;
	}

	public Iterator<Object> getMultiValueIterator() {
		if (!multiValue) {
			throw new UnsupportedOperationException("multivalue가 아닌 필드에는 Iterator를 사용할 수 없습니다.");
		}

		if (fieldsData == null) {
			return null;
		}
		return ((List<Object>) fieldsData).iterator();

	}

	public boolean isNull() {
		return fieldsData == null;
	}

	
	/*
	 * Document Index에서 사용.
	 * */
	public void readRawFrom(DataInput input) throws IOException {
		rawString = new String(input.readAString());
	}
	public void writeRawTo(DataOutput output) throws IOException {
		output.writeAString(rawString.toCharArray(), 0, rawString.length());
	}

	protected abstract Object parseData(String rawString) throws FieldDataParseException;

	public abstract void readFrom(DataInput input) throws IOException;

	/*
	 * PK에서 사용.
	 * */
	// 필드데이터를 기록. string형의 경우 size정보를 앞에 기록한다. document writer에서 사용.
	public abstract void writeTo(DataOutput output) throws IOException;

	/*
	 * Field Index에서 사용.
	 * */
	// 고정길이로 데이터만을 기록. field-index에서 필요. string형의 경우 size정보를 기록하지 않고 데이터만 저장.
	public void writeFixedDataTo(DataOutput output) throws IOException {
		writeFixedDataTo(output, 0, false);
	}
	public void writeFixedDataTo(DataOutput output, int indexSize) throws IOException {
		writeFixedDataTo(output, indexSize, false);
	}
	public abstract void writeFixedDataTo(DataOutput output, int indexSize, boolean upperCase) throws IOException;

	
	/*
	 * Group Index에서 사용.
	 * */
	// 고정길이필드는 고정으로, 가변은 가변으로 데이터만을 기록. string형의 경우 size정보를 기록하지 않고 데이터만 저장.
	public void writeDataTo(DataOutput output) throws IOException {
		writeDataTo(output, false);
	}
	public abstract void writeDataTo(DataOutput output, boolean upperCase) throws IOException;

	// 멀티밸류의 필드데이터를 하나씩 기록할수 있도록 도와주는 writer. group에서 사용됨.
	public abstract FieldDataWriter getDataWriter() throws IOException;

	//fieldData로 부터 생성하는 string
	public String getDataString(){
		if(fieldsData != null){
			return fieldsData.toString();
		}else{
			return null;
		}
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public boolean isStore() {
		return store;
	}

	public void setStore(boolean store) {
		this.store = store;
	}

	public boolean isRemoveTag() {
		return removeTag;
	}

	public void setRemoveTag(boolean removeTag) {
		this.removeTag = removeTag;
	}

	public boolean isMultiValue() {
		return multiValue;
	}

	public void setMultiValue(boolean multiValue) {
		this.multiValue = multiValue;
	}

	public Object getFieldsData() {
		return fieldsData;
	}

	public void setFieldsData(Object fieldsData) {
		this.fieldsData = fieldsData;
	}

	public boolean isFixedSize() {
		return size > 0;
	}

	@Override
	public Field clone() {
		try {
			return (Field) super.clone();
		} catch (CloneNotSupportedException e) {
			logger.error("clone error", e);
		}
		return null;
	}


}
