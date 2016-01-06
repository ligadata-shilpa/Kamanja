package com.ligadata.adapters.scratch;

import org.json.simple.JSONObject;

import lombok.extern.slf4j.Slf4j;

@Slf4j

public class ConfigValidator {
	public static boolean validateConfigs(PipelineConfig config){
		return validateInputConfig(config) && validateProcessorConfig(config) && validateOutputConfig(config);
	}
	
	public static boolean validateInputConfig(PipelineConfig config){
		boolean valid = false;
		
		return valid;
	}
	
	public static boolean validateProcessorConfig(PipelineConfig config){
		boolean valid = false;
		
		return valid;
	}

	public static boolean validateOutputConfig(PipelineConfig config){
		boolean valid = false;
		
		return valid;
	}
}
