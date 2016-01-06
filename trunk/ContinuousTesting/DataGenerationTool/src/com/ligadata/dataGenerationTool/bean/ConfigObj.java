package com.ligadata.dataGenerationTool.bean;

public class ConfigObj {

	double dataGenerationRate;
	double durationInHours;
	long sequenceID;
	boolean dropInFiles;
	boolean pushToKafka;
	String startDate;
	String endDate;
	String fileSplitPer;
	String delimiter;
	String templatePath;
	String destiniationPath;
	String compressFormat;

	public long getSequenceID() {
		return sequenceID;
	}

	public void setSequenceID(long sequenceID) {
		this.sequenceID = sequenceID;
	}

	public String getCompressFormat() {
		return compressFormat;
	}

	public void setCompressFormat(String compressFormat) {
		this.compressFormat = compressFormat;
	}

	public String getDestiniationPath() {
		return destiniationPath;
	}

	public void setDestiniationPath(String destiniationPath) {
		this.destiniationPath = destiniationPath;
	}

	public String getTemplatePath() {
		return templatePath;
	}

	public void setTemplatePath(String templatePath) {
		this.templatePath = templatePath;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public double getDataGenerationRate() {
		return dataGenerationRate;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	public boolean isDropInFiles() {
		return dropInFiles;
	}

	public void setDropInFiles(boolean dropInFiles) {
		this.dropInFiles = dropInFiles;
	}

	public double getDurationInHours() {
		return durationInHours;
	}

	public void setDurationInHours(double durationInHours) {
		this.durationInHours = durationInHours;
	}

	public boolean isPushToKafka() {
		return pushToKafka;
	}

	public void setPushToKafka(boolean pushToKafka) {
		this.pushToKafka = pushToKafka;
	}

	public String getFileSplitPer() {
		return fileSplitPer;
	}

	public void setFileSplitPer(String FileSplitPer) {
		this.fileSplitPer = FileSplitPer;
	}

	public void setDataGenerationRate(double dataGenerationRate) {
		this.dataGenerationRate = dataGenerationRate;
	}

}
