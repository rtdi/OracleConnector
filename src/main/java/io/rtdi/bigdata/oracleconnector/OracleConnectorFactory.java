package io.rtdi.bigdata.oracleconnector;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.ConnectorFactory;
import io.rtdi.bigdata.connector.connectorframework.IConnectorFactoryProducer;
import io.rtdi.bigdata.connector.connectorframework.Producer;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;

public class OracleConnectorFactory extends ConnectorFactory<OracleConnectionProperties> 
implements IConnectorFactoryProducer<OracleConnectionProperties, OracleProducerProperties> {

	public OracleConnectorFactory() {
		super("S4Connector");
	}

	@Override
	public Producer<OracleConnectionProperties, OracleProducerProperties> createProducer(ProducerInstanceController instance) throws IOException {
		return new OracleProducer(instance);
	}

	@Override
	public OracleConnectionProperties createConnectionProperties(String name) throws PropertiesException {
		return new OracleConnectionProperties(name);
	}

	@Override
	public OracleProducerProperties createProducerProperties(String name) throws PropertiesException {
		return new OracleProducerProperties(name);
	}

	@Override
	public BrowsingService<OracleConnectionProperties> createBrowsingService(ConnectionController controller) throws IOException {
		return new OracleBrowse(controller);
	}

	@Override
	public boolean supportsBrowsing() {
		return true;
	}

	static OracleConnection getDatabaseConnection(OracleConnectionProperties props) throws ConnectorCallerException {
		try {
			return getDatabaseConnection(props.getJDBCURL(), props.getUsername(), props.getPassword());
		} catch (SQLException e) {
			throw new ConnectorCallerException("Failed to establish a database connection", e, null, props.getJDBCURL());
		}
	}
	
	static OracleConnection getDatabaseConnection(String jdbcurl, String user, String passwd) throws SQLException {
        Properties info = new Properties();     
		info.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, user);
		info.put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, passwd); 

		OracleDataSource ods = new OracleDataSource();
		ods.setURL(jdbcurl);
		ods.setConnectionProperties(info);

		OracleConnection conn = (OracleConnection) ods.getConnection();
		conn.setAutoCommit(false);
		return conn;
	}

}
