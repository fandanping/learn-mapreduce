package com.neusoft.statistics.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class ConfigContextUtils {
	
	
	private static final Log LOG = LogFactory.getLog(ConfigContextUtils.class);
	private static  ConfigContextUtils instance=new ConfigContextUtils();
	private static Properties props=new Properties();
	static{
		InputStream is=ConfigContextUtils.class.getClassLoader().
		getResourceAsStream("app.properties");
		try {
			props.load(is);
			LOG.info("configContext initied finish");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				is.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	private ConfigContextUtils(){
//		LOG.info(" the instance of ConfigContext.class  is created");
	}
	public static ConfigContextUtils getInstance(){
		if(instance==null){
			ConfigContextUtils	instance=new ConfigContextUtils();
	      return instance;
		}
		return instance;
	}
	
	public String getProperty(String key){
		return props.getProperty(key);
	}
	
	public static void main(String[] args) {
		//System.out.println(getProperty("recover-default-config-info"));
	}
}
