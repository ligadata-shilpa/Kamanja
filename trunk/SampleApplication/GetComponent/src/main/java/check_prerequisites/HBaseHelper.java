package check_prerequisites;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.VersionInfo;

public class HBaseHelper {
	// Initiating configuration
	Configuration config = null;
	UserGroupInformation ugi = null;
	String tableName = "LigaDataTest";
	String errorMessage;
	String version;
	String status;
	StringWriter errors = new StringWriter();

	public void SetConfiguration(String host, String hadoopAuthentication, String hbaseAuthentication, String masterPrincipal, String regionServer) throws IOException { //for kerberos not tested
		config = HBaseConfiguration.create();
		config.setInt("zookeeper.session.timeout", 300);
		config.setInt("zookeeper.recovery.retry", 1);
		config.setInt("hbase.client.retries.number", 3);
		config.setInt("hbase.client.pause", 5000);
		config.set("hbase.zookeeper.quorum", host);
		config.set("hadoop.security.authentication", hadoopAuthentication);
		config.set("hbase.security.authentication", hbaseAuthentication);
		config.set("hbase.master.kerberos.principal", masterPrincipal);
		config.set("hbase.regionserver.kerberos.principal", regionServer);
		org.apache.hadoop.security.UserGroupInformation.setConfiguration(config);
		UserGroupInformation.loginUserFromKeytab(masterPrincipal,
				"/apps/kamanja/certificateinfo/user.keytab");
		ugi = UserGroupInformation.getLoginUser();
	}

	public void relogin() {//not tested
		try {
			if (ugi != null) {
				ugi.checkTGTAndReloginFromKeytab();
			}
		} catch (Exception e) {
			e.printStackTrace(new PrintWriter(errors));
			errorMessage = errors.toString();
		}
	}

	public void conf(String host) {
		try {
			config = HBaseConfiguration.create();
			config.setInt("zookeeper.session.timeout", 300);
			config.setInt("zookeeper.recovery.retry", 1);
			config.setInt("hbase.client.retries.number", 3);
			config.setInt("hbase.client.pause", 5000);
			config.set("hbase.zookeeper.quorum", host);
		} catch (Exception e) {
			e.printStackTrace(new PrintWriter(errors));
			errorMessage = errors.toString();
		}
	}

	@SuppressWarnings("deprecation")
	public void CreateTable() {
		try {
			// Initiating HBase table with family column
			HTableDescriptor htable = new HTableDescriptor(tableName);
			htable.addFamily(new HColumnDescriptor("person"));
			htable.addFamily(new HColumnDescriptor("contactinfo"));
			htable.addFamily(new HColumnDescriptor("creditcard"));
			System.out.println("Connecting...");
			// Initiating HBase Admin class
			HBaseAdmin hbaseAdmin = new HBaseAdmin(config);
			System.out.println("Creating Table...");
			if ((hbaseAdmin.tableExists(tableName)) == true)
				DeleteTable(tableName);
			hbaseAdmin.createTable(htable);
			// Create table in HBase
			System.out.println("Done!");
		} catch (Exception e) {
			e.printStackTrace(new PrintWriter(errors));
			errorMessage = errors.toString();
		}
	}

	public void DeleteTable(String tableName) {
		try {
			HBaseAdmin hbaseAdmin = new HBaseAdmin(config);
			hbaseAdmin.disableTable(tableName);
			hbaseAdmin.deleteTable(tableName);
		} catch (Exception e) {
			e.printStackTrace(new PrintWriter(errors));
			errorMessage = errors.toString();
		}

	}

	@SuppressWarnings("deprecation")
	public void InsertData() {
		try {
			@SuppressWarnings("deprecation")
			HTable table = new HTable(config, tableName);
			Put put = new Put(Bytes.toBytes("yousef-ligadata"));
			put.add(Bytes.toBytes("person"), Bytes.toBytes("givenName"), Bytes.toBytes("yousef"));
			put.add(Bytes.toBytes("person"), Bytes.toBytes("sureName"), Bytes.toBytes("abuElbeh"));
			put.add(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"), Bytes.toBytes("yulbeh@ligadata.com"));
			table.put(put);
			table.flushCommits();
			table.close();
		} catch (IOException e) {
			e.printStackTrace(new PrintWriter(errors));
			errorMessage = errors.toString();
		}

	}

	public void GetData() {
		try {
			HTable table = new HTable(config, tableName);
			Get get = new Get(Bytes.toBytes("yousef-ligadata"));
			get.addFamily(Bytes.toBytes("person"));
			Result result = table.get(get);
			byte[] givenName = result.getValue(Bytes.toBytes("person"), Bytes.toBytes("givenName"));
			byte[] sureName = result.getValue(Bytes.toBytes("person"), Bytes.toBytes("sureName"));
			System.out.println(
					"givenName is: " + Bytes.toString(givenName) + " and sureName is: " + Bytes.toString(sureName));
		} catch (Exception e) {
			e.printStackTrace(new PrintWriter(errors));
			errorMessage = errors.toString();
		}
	}

	public String CheckHBaseVersion() {

		// System.out.println("version is: " + VersionInfo.getVersion());
		return VersionInfo.getVersion();
	}

	public void AskHBase(String host) {
		HBaseHelper hbase = new HBaseHelper();
		hbase.conf(host);
		version = hbase.CheckHBaseVersion();
		hbase.CreateTable();
		hbase.InsertData();
		hbase.GetData();
		if (errorMessage != null)
			status = "Fail";
		else
			status = "Success";
	}

	public Configuration getConfig() {
		return config;
	}

	public void setConfig(Configuration config) {
		this.config = config;
	}

	public UserGroupInformation getUgi() {
		return ugi;
	}

	public void setUgi(UserGroupInformation ugi) {
		this.ugi = ugi;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public StringWriter getErrors() {
		return errors;
	}

	public void setErrors(StringWriter errors) {
		this.errors = errors;
	}

}
