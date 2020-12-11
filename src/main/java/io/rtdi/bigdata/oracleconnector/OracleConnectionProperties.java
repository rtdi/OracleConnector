package io.rtdi.bigdata.oracleconnector;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;

public class OracleConnectionProperties extends ConnectionProperties {

	private static final String JDBCURL = "oracle.jdbcurl";
	private static final String USERNAME = "oracle.username";
	private static final String PASSWORD = "oracle.password";

	public OracleConnectionProperties(String name) {
		super(name);
		properties.addStringProperty(JDBCURL, "JDBC URL", "The JDBC URL to use for connecting to the Oracle system", "sap-icon://target-group", "jdbc:oracle:thin:...", true);
		properties.addStringProperty(USERNAME, "Username", "Oracle database username", "sap-icon://target-group", null, true);
		properties.addPasswordProperty(PASSWORD, "Password", "Password", "sap-icon://target-group", null, true);
	}

	public String getJDBCURL() {
		return properties.getStringPropertyValue(JDBCURL);
	}
	
	public String getUsername() {
		return properties.getStringPropertyValue(USERNAME);
	}
	
	public String getPassword() {
		return properties.getPasswordPropertyValue(PASSWORD);
	}
	
	public void setJDBCURL(String value) throws PropertiesException {
		properties.setProperty(JDBCURL, value);
	}

	public void setUsername(String value) throws PropertiesException {
		properties.setProperty(USERNAME, value);
	}

	public void setPassword(String value) throws PropertiesException {
		properties.setProperty(PASSWORD, value);
	}

}
