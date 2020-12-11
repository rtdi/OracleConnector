package io.rtdi.bigdata.oracleconnector;

import static org.junit.Assert.*;

import java.sql.Connection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConnectionTestIT {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		String DB_URL= "jdbc:oracle:thin:@orasource_medium?TNS_ADMIN=C:\\\\tmp\\\\Wallet_OraSource";
		String DB_USER = "admin";
		String DB_PASSWORD = "Toor1234rtdi";
		try (Connection conn = OracleConnectorFactory.getDatabaseConnection(DB_URL, DB_USER, DB_PASSWORD);) {
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

}
