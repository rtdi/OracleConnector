package io.rtdi.bigdata.oracleconnector;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilderException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.kafka.avro.SchemaConstants;
import io.rtdi.bigdata.kafka.avro.datatypes.*;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.FileNameEncoder;
import io.rtdi.bigdata.kafka.avro.recordbuilders.AvroField;
import io.rtdi.bigdata.kafka.avro.recordbuilders.SchemaBuilder;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;

public class OracleTableMapping {
	private String oracletablename; // e.g. salesorder as L1
	private List<ColumnMapping> columnmappings; // e.g. orderid <- L1.orderid  
	private List<String> pkcolumns;
	private static ObjectMapper mapper = new ObjectMapper();
	protected Schema avroschema = null;

	private Connection conn;
	private String oracleowner;
	private String username;
	private String mappingname;
	private String deltaselect;
	private String initialselect;
	private Triggers triggerdefinitions;

	public OracleTableMapping() {
		super();
	}

	public OracleTableMapping(String mappingname, String username, String dbschema, String dbtablename, Connection conn) throws ConnectorRuntimeException {
		super();
		this.oracletablename = dbtablename;
		this.oracleowner = dbschema;
		this.username = username;
		this.conn = conn;
		this.mappingname = mappingname;
		addColumns();
	}

	private OracleTableMapping(String username, String mappingname, Connection conn) throws ConnectorRuntimeException {
		super();
		this.username = username;
		this.conn = conn;
		this.mappingname = mappingname;
	}

	public void read(File directory) throws PropertiesException {
		if (!directory.exists()) {
			throw new PropertiesException("Directory for the Relational Object Definition files does not exist", "Use the UI or create the file manually", directory.getAbsolutePath());
		} else if (!directory.isDirectory()) {
			throw new PropertiesException("Specified location exists but is no directory", (String) null, directory.getAbsolutePath());
		} else { 
			File file = new File(directory, FileNameEncoder.encodeName(mappingname + ".json"));
			if (!file.canRead()) {
				throw new PropertiesException("Properties file is not read-able", "Check file permissions and users", file.getAbsolutePath());
			} else {
				try {
					OracleTableMapping data = mapper.readValue(file, this.getClass());
				    parseValues(data);
				} catch (PropertiesException e) {
					throw e; // to avoid nesting the exception
				} catch (IOException e) {
					throw new PropertiesException("Cannot parse the json file with the properties", e, "check filename and format", file.getName());
				}
			}
		}
	}

	public void write(File directory) throws PropertiesException {
		if (!directory.exists()) {
			// throw new PropertiesException("Directory for the Relational Object Definition files does not exist", "Use the UI or create the file manually", 10005, directory.getAbsolutePath());
			directory.mkdirs();
		}
		if (!directory.isDirectory()) {
			throw new PropertiesException("Specified location exists but is no directory", (String) null, directory.getAbsolutePath());
		} else {
			File file = new File(directory, FileNameEncoder.encodeName(mappingname + ".json"));
			if (file.exists() && !file.canWrite()) { // Either the file does not exist or it exists and is write-able
				throw new PropertiesException("Properties file is not write-able", "Check file permissions and users", file.getAbsolutePath());
			} else {
				/*
				 * When writing the properties to a file, all the extra elements like description etc should not be stored.
				 * Therefore a simplified version of the property tree needs to be created.
				 */
				try {
					mapper.setSerializationInclusion(Include.NON_NULL);
					mapper.writeValue(file, this);
				} catch (IOException e) {
					throw new PropertiesException("Failed to write the json Relational Object Definition file", e, "check filename", file.getName());
				}
				
			}
		}
	}
	
	public Triggers getTriggerDefinitions() throws ConnectorRuntimeException {
		if (triggerdefinitions == null) {
			Triggers t = new Triggers();
			String sql = "select substr(trigger_name, -1) from all_triggers " + 
					"where table_owner = ? and table_name = ? and trigger_name like table_name || '\\_t\\__' escape '\\' ";
			try (PreparedStatement stmt = conn.prepareStatement(sql);) {
				stmt.setString(1, oracleowner);
				stmt.setString(2, getOracletablename());
				ResultSet rs = stmt.executeQuery();
				while (rs.next()) {
					t.setFoundTrigger(rs.getString(1));
				}
			} catch (SQLException e) {
				throw new ConnectorRuntimeException("Creating the Change Logging triggers failed in the database", e, 
						"Execute the sql as Oracle user \"" + username + "\"", sql);
			}
			triggerdefinitions = t;
		}
		return triggerdefinitions;
	}

	void createTrigger() throws ConnectorRuntimeException {
		Triggers t = getTriggerDefinitions();
		t.createTriggers();
	}

	protected void parseValues(OracleTableMapping data) throws ConnectorRuntimeException {
		this.oracletablename = data.getOracletablename();
		this.columnmappings = data.getColumnmappings();
		this.pkcolumns = data.getPKColumns();
		this.oracleowner = data.getOracleowner();
	}

	public void setOracletablename(String tablename) {
		this.oracletablename = tablename;
	}

	public void setOracleowner(String schemaname) {
		this.oracleowner = schemaname;
	}

	public void addColumns() throws ConnectorRuntimeException {
		String sql = "select c.column_name, c.data_type, c.data_length, c.data_scale, pc.position \r\n"
				+ "from all_tab_columns c \r\n"
				+ "	left outer join all_constraints p \r\n"
				+ "		on (p.constraint_type = 'P' and p.owner = c.owner and p.table_name = c.table_name) \r\n"
				+ "	left outer join all_cons_columns pc\r\n"
				+ "		on (pc.owner = p.owner and pc.constraint_name = p.constraint_name and pc.column_name = c.column_name) \r\n"
				+ "where c.owner = ? and c.table_name = ? \r\n"
				+ "order by pc.position";
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			stmt.setString(1, oracleowner);
			stmt.setString(2, oracletablename);
			ResultSet rs = stmt.executeQuery();
			int columncount = 0;
			while (rs.next()) {
				ColumnMapping m = addMapping(rs.getString(1), "\"" + rs.getString(1) + "\"", getOracleDataType(rs.getString(2), rs.getInt(3), rs.getInt(4)));
				if (rs.getInt(5) != 0) {
					addPK(rs.getInt(5), m);
				}
				columncount++;
			}
			if (columncount == 0) {
				throw new ConnectorRuntimeException("This table does not seem to exist in the Oracle database itself", null, 
						"Execute the sql as Oracle user \"" + username + "\"", sql);
			}
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Reading the table definition failed", e, 
					"Execute the sql as Oracle user \"" + username + "\"", sql);
		}
	}

	public void addPK(int pos, ColumnMapping m) {
		if (pkcolumns == null) {
			pkcolumns = new ArrayList<>();
		}
		if (pkcolumns.size() < pos) {
			while (pkcolumns.size() < pos-1) {
				pkcolumns.add(null);
			}
			pkcolumns.add(m.getAlias());
		} else {
			pkcolumns.set(pos, m.getAlias());
		}
	}

	private static String getOracleDataType(String datatype, int length, int scale) {
		switch (datatype) {
		case "NUMBER": // number(p, s) with p between 1..38 and scale 0..p
			return datatype + "(" + length + ", " + scale + ")";
		case "CHAR":
		case "VARCHAR":
		case "VARCHAR2":
		case "RAW":
		case "NCHAR":
		case "NVARCHAR":
			return datatype + "(" + length + ")";
		default:
			return datatype;
		}
	}

	public ColumnMapping addMapping(String columnname, String sqlexpression, String oracledatatype) {
		if (columnmappings == null) {
			columnmappings = new ArrayList<>();
		}
		ColumnMapping m = new ColumnMapping(columnname, sqlexpression, oracledatatype);
		columnmappings.add(m);
		return m;
	}

	public static OracleTableMapping readDefinition(String username, String name, Connection conn, File directory) throws PropertiesException {
		OracleTableMapping o = new OracleTableMapping(username, name, conn);
		o.read(directory);
		return o;
	}
	
	@JsonIgnore
	public void setConnection(OracleBrowse browser) {
		this.username = browser.getConnectionProperties().getUsername();
		this.conn = browser.getConnection();
	}

	@JsonIgnore
	protected Connection getConn() {
		return conn;
	}

	public String getOracleowner() {
		return oracleowner;
	}

	@JsonIgnore
	protected String getUsername() {
		return username;
	}

	
	@JsonIgnore
	public Schema getAvroSchema() throws SchemaBuilderException, ConnectorRuntimeException {
		if (avroschema == null) {
			ValueSchema v = new ValueSchema(getName(), null);
			createSchema(v);
			v.build();
		}
		return avroschema;
	}
	
	public void createDeltaObjects() throws ConnectorRuntimeException, SQLException {
		createTrigger();
		deltaselect = createSelectDelta().toString();
		initialselect = createSelectInitial().toString();
	}

	private StringBuffer createSelectDelta() {
		StringBuffer conditions = createRootJoinCondition(this);
		StringBuffer select = new StringBuffer();
		select.append("select ");
		select.append("case when d.\"");
		select.append(getPKColumns().get(0));
		select.append("\" is null then 'D' else 'A' end as \"_CHANGE_TYPE\", \r\n");
		select.append("l.\"_SCN\" as \"_SCN\",\r\n");
		select.append("d.rowid as \"").append(SchemaConstants.SCHEMA_COLUMN_SOURCE_ROWID).append("\", \r\n");
		select.append(createProjectionDelta(this, true));
		select.append("\r\nfrom (select max(scn) as \"_SCN\", ");
		for (int i = 0; i < getPKColumns().size(); i++) {
			if ( i != 0) {
				select.append(", ");
			}
			select.append("PK");
			select.append(i+1);
			select.append(" as \"");
			select.append(getPKColumns().get(i));
			select.append("\"");
		}
		select.append(" from pklog ");
		select.append(" where scn > ? and scn < ? and table_name = '" 
				+ getOracletablename() + "' and schema_name = '" + oracleowner + "'\r\n");
		select.append("group by ");
		for (int i = 0; i < getPKColumns().size(); i++) {
			if ( i != 0) {
				select.append(", ");
			}
			select.append("PK");
			select.append(i+1);
		}
		select.append(") l \r\n");
		select.append("left outer join \"");
		select.append(oracleowner);
		select.append("\".\"");
		select.append(getOracletablename());
		select.append("\" d\r\n");
		select.append("on (");
		select.append(conditions);
		select.append(")");
		return select;
	}

	private StringBuffer createSelectInitial() {
		StringBuffer select = new StringBuffer();
		select.append("select 'I' as \"_CHANGE_TYPE\", \r\n");
		select.append("null as \"_PRCESSED_SEQ\", \r\n");
		select.append("d.rowid as \"").append(SchemaConstants.SCHEMA_COLUMN_SOURCE_ROWID).append("\", \r\n");
		select.append(createProjectionInitial(this));
		select.append("\r\nfrom \"");
		select.append(oracleowner);
		select.append("\".\"");
		select.append(getOracletablename());
		select.append("\" d");
		return select;
	}

	public String getOracletablename() {
		return oracletablename;
	}

	protected String getPKList() {
		StringBuffer b = new StringBuffer();
		for (int i = 0; i < getPKColumns().size(); i++) {
			String columnname = getPKColumns().get(i);
			if (i != 0) {
				b.append(", ");
			}
			b.append('"');
			b.append(columnname);
			b.append('"');
		}
		return b.toString();
	}

	public static boolean checktable(String tablename, Connection conn) throws ConnectorRuntimeException {
		String sql = "select 1 from all_tables where table_name = ? and owner = user";
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			stmt.setString(1, tablename);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				return true;
			} else {
				return false;
			}
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Checking if the table exists failed in the database", e, 
					"Execute the sql as Oracle user", sql);
		}
	}

	private static StringBuffer createRootJoinCondition(OracleTableMapping r) {
		StringBuffer conditions = new StringBuffer();
		for (int i = 0; i < r.getPKColumns().size(); i++) {
			String columnname = r.getPKColumns().get(i);
			if (i != 0) {
				conditions.append(" and ");
			}
			conditions.append("l.\"");
			conditions.append(columnname);
			conditions.append("\" = d.\"");
			conditions.append(columnname);
			conditions.append("\"");
		}
		return conditions;
	}

	private static StringBuffer createProjectionDelta(OracleTableMapping r, boolean usedriver) {
		StringBuffer b = new StringBuffer();
		for (int i = 0; i < r.getColumnmappings().size(); i++) {
			ColumnMapping column = r.getColumnmappings().get(i);
			if (i != 0) {
				b.append(", ");
			}
			if (usedriver && r.getPKColumns().contains(column.getTableColumnName())) {
				b.append("l.");
				b.append(column.getTableColumnName());
				b.append(" as \"");
				b.append(column.getAlias());
				b.append("\"");
			} else {
				b.append(column.getSql());
				b.append(" as \"");
				b.append(column.getAlias());
				b.append("\"");
			}
		}
		return b;
	}

	private static StringBuffer createProjectionInitial(OracleTableMapping r) {
		StringBuffer b = new StringBuffer();
		for (int i = 0; i < r.getColumnmappings().size(); i++) {
			ColumnMapping column = r.getColumnmappings().get(i);
			if (i != 0) {
				b.append(", ");
			}
			b.append(column.getSql());
			b.append(" as \"");
			b.append(column.getAlias());
			b.append("\"");
		}
		return b;
	}

	public String getName() {
		return mappingname;
	}

	public void setName(String name) {
		this.mappingname = name;
	}

	public List<ColumnMapping> getColumnmappings() {
		return columnmappings;
	}

	public void setColumnmappings(List<ColumnMapping> columnmappings) {
		this.columnmappings = columnmappings;
	}

	protected void createSchema(SchemaBuilder valueschema) throws ConnectorRuntimeException {
		try {
			if (getColumnmappings() != null) {
				for ( ColumnMapping m : getColumnmappings()) {
					String oracledatatypestring = m.getOracledatatype();
					String columnname = m.getAlias();
					AvroField f = valueschema.add(columnname, getDataType(oracledatatypestring), null, true);
					if (pkcolumns != null && pkcolumns.contains(m.getTableColumnName())) {
						f.setPrimaryKey();
					}
				}
				avroschema = valueschema.getSchema();
			} else {
				throw new ConnectorRuntimeException("The schema definition file does not contain any columns!", null, 
						"Something was wrong when the schema mapping file got created", this.getName());
			}
		} catch (SchemaBuilderException e) {
			throw new ConnectorRuntimeException("The Avro Schema cannot be created due to an internal error", e, 
					"Please create an issue", valueschema.toString());
		}
	}

	public List<String> getPKColumns() {
		return pkcolumns;
	}

	public static Schema getDataType(String datatypestring) throws ConnectorRuntimeException {
		Pattern p = Pattern.compile("(\\w*)\\s*\\(?\\s*(\\d*)\\s*\\,?\\s*(\\d*)\\s*\\)?.*");  // decimal(10,3) plus spaces anywhere, three groups
		Matcher m = p.matcher(datatypestring);
		m.matches();
		String datatype = m.group(1);
		String lengthstring = m.group(2);
		int length = 0;
		int scale = 0;
		if (lengthstring != null && lengthstring.length() != 0) {
			length = Integer.valueOf(lengthstring);
			String scalestring = m.group(3);
			if (scalestring != null && scalestring.length() != 0) {
				scale = Integer.valueOf(scalestring);
			}
		}
		switch (datatype) {
		case "CHAR":
		case "NCHAR":
		case "VARCHAR":
		case "VARCHAR2": // depending on the database codepage can be any unicode character
		case "NVARCHAR2":
			return AvroNVarchar.getSchema(length);
		case "NUMBER":
			return AvroDecimal.getSchema(length, scale);
		case "FLOAT":
		case "BINARY_FLOAT": // 32-bit floating-point number
			return AvroFloat.getSchema();
		case "BINARY_DOUBLE": // 64-bit floating-point number
			return AvroDouble.getSchema();
		case "DATE":
			return AvroDate.getSchema();
		case "TIMESTAMP": // handles all kinds of timestamp variants
			return AvroTimestampMicros.getSchema();
		case "BLOB":
		case "BFILE":
			return AvroBytes.getSchema();
		case "LONG":
		case "LONG RAW":
		case "CLOB":
		case "NCLOB":
			return AvroNCLOB.getSchema();
		case "RAW":
			return AvroFixed.getSchema(length);
		case "ROWID":
			return AvroVarchar.getSchema(18);
		case "UROWID":
			return AvroVarchar.getSchema(length);
		case "XMLType":
			return AvroNCLOB.getSchema();
		case "UriType":
			return AvroUri.getSchema();
		case "INTERVAL": // handles all kinds of interval types and lengths
			return AvroFloat.getSchema();
		case "ST_POINT":
			return AvroSTPoint.getSchema();
		case "ST_GEOMETRY":
			return AvroSTGeometry.getSchema();
		default:
			throw new ConnectorRuntimeException("Table contains a data type which is not known", null, "Newer Oracle version??", datatype);
		}
	}

	@JsonIgnore
	public String getDeltaSelect() {
		return deltaselect;
	}
	
	public static class ColumnMapping {
		private String alias;
		private String sql;
		private String oracledatatype;
		private String tablecolumnname;

		public ColumnMapping() {
		}
		
		public ColumnMapping(String alias, String sqlexpression, String oracledatatype) {
			this.alias = alias;
			this.oracledatatype = oracledatatype;
			setSql(sqlexpression);
		}
		public String getAlias() {
			return alias;
		}
		
		public void setAlias(String alias) {
			this.alias = alias;
		}
		
		public String getSql() {
			return sql;
		}
		
		public void setSql(String sql) {
			this.sql = sql;
			String[] comp = sql.split("\\.");
			if (comp.length == 2) {
				tablecolumnname = comp[1];
			} else {
				tablecolumnname = comp[0];
			}
			if (tablecolumnname.charAt(0) == '"') {
				tablecolumnname = tablecolumnname.substring(1, tablecolumnname.length()-1);
			}
		}

		public String getOracledatatype() {
			return oracledatatype;
		}

		public void setOracledatatype(String oracledatatype) {
			this.oracledatatype = oracledatatype;
		}

		@Override
		public String toString() {
			return alias;
		}

		protected String getTableColumnName() {
			return tablecolumnname;
		}


	}

	@JsonIgnore
	public String getInitialSelect() {
		return initialselect;
	}

	@Override
	public String toString() {
		return oracletablename;
	}

	public class Triggers {
		private String[] trigger = new String[3];
		private boolean[] exists = new boolean[3];
		
		public Triggers() throws ConnectorRuntimeException {
			exists[0] = false;
			exists[1] = false;
			exists[2] = false;
			String sourceidentifier = "\"" + oracleowner + "\".\"" + getOracletablename() + "\"";
			StringBuffer pklist1 = new StringBuffer();
			StringBuffer pklist2 = new StringBuffer();
			StringBuffer pklist3 = new StringBuffer();
			StringBuffer pklistdifferent = new StringBuffer();
			for (int i = 0; i < getPKColumns().size(); i++) {
				String pkcolumn = getPKColumns().get(i);
				if (pkcolumn == null) {
					throw new ConnectorRuntimeException("The table is not using all primary key columns", null, 
							"Make sure all pk columns are mapped at least", getOracletablename() + ": " + getPKColumns().toString());
				}
				if (i != 0) {
					pklist1.append(',');
					pklist2.append(',');
					pklist3.append(',');
					pklistdifferent.append(" OR ");
				}
				// :c."MANDT",:c."VBELN"
				pklist1.append(":c.\"");
				pklist1.append(pkcolumn);
				pklist1.append('"');
				// PK1,PK2
				pklist2.append("PK");
				pklist2.append(i+1);
				// :o."MANDT",:o."VBELN"
				pklist3.append(":c.\"");
				pklist3.append(pkcolumn);
				pklist3.append('"');
				// :o."MANDT" <> :c."MANDT" OR :o."VBELN" <> :c."VBELN"
				pklistdifferent.append(":o.\"");
				pklistdifferent.append(pkcolumn);
				pklistdifferent.append("\" <> :c.\"");
				pklistdifferent.append(pkcolumn);
				pklistdifferent.append('"');
			}
			trigger[0] = "CREATE TRIGGER \"" + getOracletablename() + "_t_i\" \r\n" + 
					" AFTER INSERT ON " + sourceidentifier + " \r\n" + 
					" REFERENCING NEW as c \r\n" + 
					" FOR EACH ROW \r\n" + 
					" BEGIN \r\n" + 
					"     INSERT INTO \"" + username + "\".PKLOG \r\n" +
					"       (change_ts, schema_name, table_name, change_type, \r\n" +
					"       scn, \r\n" +
					"      " + pklist2.toString() + ") \r\n" + 
					"     VALUES (current_timestamp, '" + oracleowner + "', '" + getOracletablename() + "', 'I', \r\n" +
					"       dbms_flashback.get_system_change_number, \r\n" +
					"       " + pklist1.toString() + " ); \r\n" + 
					" END;";
			trigger[1] =   "CREATE TRIGGER \"" + getOracletablename() + "_t_u\" \r\n" + 
					" AFTER UPDATE ON " + sourceidentifier + " \r\n" + 
					" REFERENCING NEW as c OLD as o \r\n" + 
					" FOR EACH ROW \r\n" + 
					" BEGIN \r\n" + 
					"     INSERT INTO \"" + username + "\".PKLOG \r\n" +
					"       (change_ts, schema_name, table_name, change_type, \r\n" +
					"       scn, \r\n" +
					"      " + pklist2.toString() + ") \r\n" + 
					"     VALUES (current_timestamp, '" + oracleowner + "', '" + getOracletablename() + "', 'U', \r\n" +
					"       dbms_flashback.get_system_change_number, \r\n" +
					"       " + pklist1.toString() + " ); \r\n" + 
					"     IF (" + pklistdifferent.toString() + " ) THEN \r\n" + 
					"       INSERT INTO \"" + username + "\".PKLOG \r\n" +
					"         (change_ts, schema_name, table_name, change_type, \r\n" +
					"       scn, \r\n" +
					"        " + pklist2.toString() + ") \r\n" + 
					"       VALUES (current_timestamp, '" + oracleowner + "', '" + getOracletablename() + "', 'U', \r\n" +
					"       dbms_flashback.get_system_change_number, \r\n" +
					"         " + pklist3.toString() + " ); \r\n" + 
					"     END IF; \r\n" +
					"END;";
			trigger[2] =   "CREATE TRIGGER \"" + getOracletablename() + "_t_d\" \r\n" + 
					" AFTER DELETE ON " + sourceidentifier + " \r\n" + 
					" REFERENCING OLD as c \r\n" + 
					" FOR EACH ROW \r\n" + 
					" BEGIN \r\n" + 
					"     INSERT INTO \"" + username + "\".PKLOG \r\n" +
					"      (change_ts, schema_name, table_name, change_type, \r\n" +
					"       scn, \r\n" +
					"      " + pklist2.toString() + ") \r\n" + 
					"     VALUES (current_timestamp, '" + oracleowner + "', '" + getOracletablename() + "', 'D', \r\n" +
					"       dbms_flashback.get_system_change_number, \r\n" +
					"       " + pklist1.toString() + " ); \r\n" + 
					"END;";
		}
		
		public void createTriggers() throws ConnectorRuntimeException {
			for (int i=0; i<3; i++) {
				if (!exists[i]) {
					createTrigger(trigger[i]);
				}
			}
		}
		
		private void createTrigger(String sql) throws ConnectorRuntimeException {
			try {
				try (Statement stmttr = conn.createStatement();) {
					stmttr.execute(sql);
				}
			} catch (SQLException e) {
				throw new ConnectorRuntimeException("Creating the Change Logging triggers failed in the database", e, 
						"Execute the sql as Oracle user \"" + username + "\"", sql);
			}
		}

		public void setFoundTrigger(String suffix) {
			switch (suffix) {
			case "i":
				exists[0] = true;
				break;
			case "u":
				exists[1] = true;
				break;
			case "d":
				exists[2] = true;
				break;
			}
		}
		
		public String getSQLScript() {
			StringBuffer b = new StringBuffer();
			for (int i=0; i<3; i++) {
				if (exists[i]) {
					b.append("/* Trigger ").append(oracleowner).append(".").append(oracletablename).append("_t_");
					switch (i) {
					case 0:
						b.append("i");
						break;
					case 1:
						b.append("u");
						break;
					case 2:
						b.append("d");
						break;
					}
					b.append(" exists already\r\n");
				}
				b.append(trigger[i]);
				if (exists[i]) {
					b.append("*/\r\n");
				}
				b.append("\r\n");
			}
			return b.toString();
		}
	}

}
