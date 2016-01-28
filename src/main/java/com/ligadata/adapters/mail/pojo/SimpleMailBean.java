package com.ligadata.adapters.mail.pojo;


import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.log4j.Log4j;

import org.json.simple.JSONObject;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

import com.ligadata.adapters.AdapterConfiguration;
import com.ligadata.adapters.mail.st4.CustomErrorListener;
import com.ligadata.adapters.mail.st4.JSONAdapter;

@AllArgsConstructor
@NoArgsConstructor
@Log4j
@ToString
public class SimpleMailBean {
	
	@Setter @Getter
	AdapterConfiguration conf;
	
	@Getter @Setter
	String from;
	@Getter @Setter
	String to;
	@Getter @Setter
	String cc;
	@Getter @Setter
	String bcc;
	
	@Getter @Setter
	String subjectHTML;
	@Getter @Setter
	String headerHTML;
	@Getter @Setter
	String bodyHTML;
	@Getter @Setter
	String footerHTML;
	
	@Getter @Setter
	String templateDirectory;
	@Getter @Setter
	JSONObject fillers;
	
	@Getter @Setter
	String templateName;
	
	STGroup stGroup;
	
	public void populateData(){
		if(templateDirectory != null && !templateDirectory.isEmpty()){
			Path templateDir = FileSystems.getDefault().getPath(templateDirectory);
			if(Files.exists(templateDir)){
				
				templateName = Constants.ALERT_TEMPLATE_MAP.get((String)fillers.get("alertType"));
				if(templateName != null && !templateName.isEmpty()){
					stGroup = new STGroupFile(templateDir.toAbsolutePath().toString()+"/"+templateName+".stg");
					System.out.println(stGroup.show());
				}else{
					templateName = Constants.DEFAULT_TEMPLATE;
					stGroup = new STGroupFile(templateDir.toAbsolutePath().toString()+"/"+templateName+".stg");
					System.out.println(stGroup.show());
				}
				
				//Register Listener and Adapters here
				stGroup.setListener(new CustomErrorListener());
				stGroup.registerModelAdaptor(JSONObject.class, new JSONAdapter());
				
				ST subject = stGroup.getInstanceOf("subject");
				subject.add("x",fillers);
				subjectHTML = subject.render();
				
				ST header = stGroup.getInstanceOf("header");
				header.add("x",fillers);
				headerHTML = header.render();
				
				ST body = stGroup.getInstanceOf("body");
				body.add("x",fillers);
				bodyHTML = body.render();
				
				ST footer = stGroup.getInstanceOf("footer");
				footer.add("x",fillers);
				
				ST testFooter = stGroup.getInstanceOf("testFooter");
				testFooter.add("x",fillers);
				
				//Check if it is a test Message
				if(fillers.containsKey("testMessage"))
					footerHTML = testFooter.render();
				else 
					footerHTML = footer.render();
				
				//Populate the other fields from JSON
				//Currently doing a direct filling, later it may be rule based (based on AlertType)
				if(conf.getProperty(AdapterConfiguration.TEST_FLAG).equalsIgnoreCase("true")){
					to = conf.getProperty(AdapterConfiguration.TESTMAIL_RECEIPENTS);
				}else{
					to = (String)fillers.get("associateEmail");
					cc = (String)fillers.get("supervisorEmail");
					from = conf.getProperty(AdapterConfiguration.MAIL_FROM);
				}
				
			}else{
				log.error(Constants.ERROR_TEMPLATE_DIRECTORY_UNAVAILABLE+templateDir.toAbsolutePath().toString());
			}
		}
	}
}
