package io.rtdi.bigdata.oracleconnector.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;

import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.entity.TableEntry;
import io.rtdi.bigdata.connector.connectorframework.servlet.UI5ServletAbstract;
import io.rtdi.bigdata.oracleconnector.OracleBrowse;
import io.rtdi.bigdata.oracleconnector.OracleProducer;
import io.rtdi.bigdata.oracleconnector.OracleTableMapping;
import io.rtdi.bigdata.oracleconnector.OracleTableMapping.Triggers;

@WebServlet("/ui5/TriggerScript")
public class TriggerScript extends UI5ServletAbstract {
	private static final long serialVersionUID = 1L;

    public TriggerScript() {
        super("TriggerScript", "TriggerScript");
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		long expiry = System.currentTimeMillis() + BROWSER_CACHING_IN_SECS*1000;
		response.setDateHeader("Expires", expiry);
		response.setHeader("Cache-Control", "max-age="+ BROWSER_CACHING_IN_SECS);
		PrintWriter out = response.getWriter();
		out.println("<!DOCTYPE html>");
		out.println("<html style=\"height: 100%;\">");
		out.println("<head>");
		out.println("<meta charset=\"ISO-8859-1\">");
		out.print("<title>");
		out.print(getTitle());
		out.println("</title>");
		out.println("</head>");
		out.println("<body><pre>");

		String connectionname = request.getParameter("connectionname");
		ConnectorController connector = WebAppController.getConnectorOrFail(request.getServletContext());
		ConnectionController connection = connector.getConnectionOrFail(connectionname);
		OracleBrowse browser = (OracleBrowse) connection.getBrowser();
		
		try {
			browser.open();
			out.print("/* The PKLOG table must exist in schema: ");
			try {
				out.print(browser.getConnection().getSchema());
			} catch (SQLException e) {
			}
			out.println();
			out.print(OracleProducer.getPKLOGTable());
			out.println(";");
			out.println("*/");
			out.println();
			for (TableEntry table : browser.getRemoteSchemaNames()) {
				OracleTableMapping o = browser.getBusinessObject(table.getTablename());
				Triggers t = o.getTriggerDefinitions();
				out.print(t.getSQLScript());
			}
		} finally {
			browser.close();
		}

		out.println("</pre></body>");
		out.println("</html>");
	}

}
