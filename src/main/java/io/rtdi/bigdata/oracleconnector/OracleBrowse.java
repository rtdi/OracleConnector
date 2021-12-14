package io.rtdi.bigdata.oracleconnector;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilderException;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.entity.TableEntry;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.FileNameEncoder;

public class OracleBrowse extends BrowsingService<OracleConnectionProperties> {
	
	private File bopath;
	private Connection conn;

	public OracleBrowse(ConnectionController controller) throws IOException {
		super(controller);
		bopath = new File(controller.getDirectory(), "BusinessObjects");
	}

	@Override
	public void open() throws IOException {
		conn = OracleConnectorFactory.getDatabaseConnection(getConnectionProperties());
	}

	@Override
	public void close() {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
			}
			conn = null;
		}
	}

	@Override
	public List<TableEntry> getRemoteSchemaNames() throws IOException {
		if (bopath.isDirectory()) {
			File[] files = bopath.listFiles();
			List<TableEntry> ret = new ArrayList<>();
			if (files != null) {
				for (File f : files) {
					if (f.getName().endsWith(".json") && f.isFile()) {
						String name = FileNameEncoder.decodeName(f.getName());
						ret.add(new TableEntry(name.substring(0, name.length()-5))); // remove the .json ending
					}
				}
			}
			return ret;
		} else {
			return null;
		}
	}

	@Override
	public Schema getRemoteSchemaOrFail(String name) throws IOException {
		OracleTableMapping n1 = OracleTableMapping.readDefinition(null, name, null, bopath);
		try {
			return n1.getAvroSchema();
		} catch (SchemaBuilderException e) {
			throw new ConnectorRuntimeException("Schema cannot be parsed", e, null, null);
		}
	}

	public OracleTableMapping getBusinessObject(String name) throws IOException {
		String username = null;
		if (conn != null) {
			try {
				username = conn.getSchema();
			} catch (SQLException e) {
			}
		}
		OracleTableMapping n1 = OracleTableMapping.readDefinition(username, name, conn, bopath);
		return n1;
	}

	public File getBusinessObjectDirectory() {
		return bopath;
	}
	
	public List<TableImport> getOracleTables() throws ConnectorRuntimeException {
		/*
		 * Get all tables that are not Oracle maintained, hence ignoring SYS, SYSTEM,...
		 * If the user has CREATE-ANY-TRIGGER then all schemas are shown.
		 * If the user has just CREATE-TRIGGER privs, only the own tables are shown.
		 */
		String sql = "select owner, table_name from all_tables \r\n"
				+ "  where owner not in (select username from all_users where oracle_maintained = 'Y') and \r\n"
				+ "    ( exists (select privilege from USER_SYS_PRIVS where privilege = 'CREATE ANY TRIGGER') \r\n"
				+ "      or (exists (select privilege from USER_SYS_PRIVS where privilege = 'CREATE TRIGGER') \r\n"
				+ "          and owner = user))";
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			ResultSet rs = stmt.executeQuery();
			List<TableImport> sortedlist = new ArrayList<>();
			while (rs.next()) {
				String schemaname = rs.getString(1);
				String tablename = rs.getString(2);
				sortedlist.add(new TableImport(schemaname, tablename));
			}
			return sortedlist;
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Reading all tables of the ALL_TABLES view failed", e, 
					"Execute the sql as Oracle user \"" + getConnectionProperties().getUsername() + "\"", sql);
		}
	}
	
	public Connection getConnection() {
		return conn;
	}
	
	public static class TableImport {
		private String oracletable;
		private String oracleowner;
		private boolean imported;
		private String mappingname;
		
		public TableImport() {
			super();
		}

		public TableImport(String hanaschema, String hanatable) {
			super();
			this.oracletable = hanatable;
			this.oracleowner = hanaschema;
			this.mappingname = hanaschema + "_" + hanatable;
		}

		public String getOracletablename() {
			return oracletable;
		}
		public void setOracletablename(String tablename) {
			this.oracletable = tablename;
		}
		public boolean isImported() {
			return imported;
		}
		public void setImported(boolean imported) {
			this.imported = imported;
		}

		public String getOracleowner() {
			return oracleowner;
		}

		public void setOracleowner(String schemaname) {
			this.oracleowner = schemaname;
		}

		public String getMappingname() {
			return mappingname;
		}

		public void setMappingname(String mappingname) {
			this.mappingname = mappingname;
		}
	}

	@Override
	public void validate() throws IOException {
		close();
		open();
		String sql = "select table_name from all_tables where rownum = 1";
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				return;
			} else {
				throw new ConnectorRuntimeException("No entries found in the Oracle ALL_TABLES view - missing permissions?", null, 
						"Execute the sql as Hana user \"" + getConnectionProperties().getUsername() + "\"", sql);
			}
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Reading the Oracle ALL_TABLES view failed", e, 
					"Execute the sql as Hana user \"" + getConnectionProperties().getUsername() + "\"", sql);
		} finally {
			close();
		}
		
	}

	@Override
	public void deleteRemoteSchemaOrFail(String remotename) throws IOException {
		File file = new File(bopath, remotename + ".json");
		java.nio.file.Files.delete(file.toPath());
	}
}
