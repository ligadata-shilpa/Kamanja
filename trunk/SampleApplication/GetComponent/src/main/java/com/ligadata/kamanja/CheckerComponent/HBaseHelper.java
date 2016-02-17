package com.ligadata.kamanja.CheckerComponent;

import java.io.IOException;
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
    private String namespace = "default";
    private String tableBaseName = "KamanjaPreReqTest";
    String errorMessage;
    String version;
    String status;
    StringWriter errors = new StringWriter();
    StringUtility strutl = new StringUtility();
    HBaseAdmin hbaseAdmin;

    public void SetConfiguration(String host, String authentication, String masterPrincipal, String regionServer,
                                 String keyType, String principal) { // for
        // kerberos
        // not
        // tested
        try {
            config = HBaseConfiguration.create();
            config.setInt("zookeeper.session.timeout", 5000);
            config.setInt("zookeeper.recovery.retry", 1);
            config.setInt("hbase.client.retries.number", 3);
            config.setInt("hbase.client.pause", 5000);
            config.set("hbase.zookeeper.quorum", host);
            if (((authentication != null) && authentication.equalsIgnoreCase("kerberos"))) {
                config.set("hadoop.security.authorization", "true");
                config.set("hadoop.proxyuser.hdfs.groups", "*");
                config.set("hadoop.security.authentication", authentication);
                config.set("hbase.security.authentication", authentication);
                config.set("hbase.master.kerberos.principal", masterPrincipal);
                config.set("hbase.regionserver.kerberos.principal", regionServer);
                org.apache.hadoop.security.UserGroupInformation.setConfiguration(config);
                UserGroupInformation.loginUserFromKeytab(principal, keyType);
                ugi = UserGroupInformation.getLoginUser();
            }
        } catch (Exception e) {
            // e.printStackTrace(new PrintWriter(errors));
            // errorMessage = errors.toString();
            errorMessage += strutl.getStackTrace(e);
        }
    }

    public void relogin() {// not tested
        try {
            if (ugi != null) {
                ugi.checkTGTAndReloginFromKeytab();
            }
        } catch (Exception e) {
            // e.printStackTrace(new PrintWriter(errors));
            // errorMessage = errors.toString();
            errorMessage += strutl.getStackTrace(e);
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
            // e.printStackTrace(new PrintWriter(errors));
            // errorMessage = errors.toString();
            errorMessage += strutl.getStackTrace(e);
        }
    }

    // @SuppressWarnings({ "resource", "deprecation" })
    public void CreateTable() {
        try {
            relogin();// for kerberos
            // Initiating HBase table with family column
            HTableDescriptor htable = new HTableDescriptor(getTableName());
            htable.addFamily(new HColumnDescriptor("person"));
            htable.addFamily(new HColumnDescriptor("contactinfo"));
            htable.addFamily(new HColumnDescriptor("creditcard"));
            // System.out.println("Connecting...");
            // Initiating HBase Admin class
            hbaseAdmin = new HBaseAdmin(config);
            // System.out.println("Creating Table...");
            // System.out.println(hbaseAdmin.tableExists(tableName));
            // DeleteTable(tableName);
            if ((hbaseAdmin.tableExists(getTableName())) == true) {
                DeleteTable(getTableName());
            }
            // System.out.println(hbaseAdmin.tableExists(tableName));
            hbaseAdmin.createTable(htable);
            // Create table in HBase
            // System.out.println("Done!");
        } catch (Exception e) {
            // e.printStackTrace(new PrintWriter(errors));
            // errorMessage = errors.toString();
            errorMessage = strutl.getStackTrace(e);
        }
    }

    @SuppressWarnings({"resource", "deprecation"})
    public void DeleteTable(String tableName) {
        try {
            relogin();// for kerberos
            // hbaseAdmin = new HBaseAdmin(config);
            hbaseAdmin.disableTable(tableName);
            hbaseAdmin.deleteTable(tableName);
        } catch (Exception e) {
            // e.printStackTrace(new PrintWriter(errors));
            // errorMessage = errors.toString();
            errorMessage += strutl.getStackTrace(e);
        }

    }

    @SuppressWarnings("deprecation")
    public void InsertData() {
        try {
            relogin();// for kerberos
            HTable table = new HTable(config, getTableName());
            Put put = new Put(Bytes.toBytes("yousef-ligadata"));
            put.add(Bytes.toBytes("person"), Bytes.toBytes("givenName"), Bytes.toBytes("yousef"));
            put.add(Bytes.toBytes("person"), Bytes.toBytes("sureName"), Bytes.toBytes("abuElbeh"));
            put.add(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"), Bytes.toBytes("yulbeh@ligadata.com"));
            table.put(put);
            table.flushCommits();
            table.close();
        } catch (IOException e) {
            // e.printStackTrace(new PrintWriter(errors));
            // errorMessage = errors.toString();
            errorMessage += strutl.getStackTrace(e);
        }

    }

    @SuppressWarnings({"resource", "deprecation"})
    public String GetData() {
        try {
            relogin();// for kerberos
            HTable table = new HTable(config, getTableName());
            Get get = new Get(Bytes.toBytes("yousef-ligadata"));
            get.addFamily(Bytes.toBytes("person"));
            Result result = table.get(get);
            byte[] givenName = result.getValue(Bytes.toBytes("person"), Bytes.toBytes("givenName"));
            // byte[] sureName = result.getValue(Bytes.toBytes("person"),
            // Bytes.toBytes("sureName"));
            // System.out.println(
            // "givenName is: " + Bytes.toString(givenName) + " and sureName is:
            // " + Bytes.toString(sureName));
            return givenName.toString();
        } catch (Exception e) {
            // e.printStackTrace(new PrintWriter(errors));
            // errorMessage = errors.toString();
            errorMessage += strutl.getStackTrace(e);
        }
        return null;
    }

    public String CheckHBaseVersion() {

        // System.out.println("version is: " + VersionInfo.getVersion());
        return VersionInfo.getVersion();
    }

    public void AskHBase(String host, String authentication, String masterPrincipal, String regionServer,
                         String keyType, String principal, String nsp) throws IOException {
        // HBaseHelper hbase = new HBaseHelper();
        SetConfiguration(host, authentication, masterPrincipal, regionServer, keyType, principal); // kerberos not tested yet
        if (nsp != null)
            namespace = nsp.trim();
        //conf(host);
        version = CheckHBaseVersion();
        CreateTable();
        // DeleteTable(tableName);
        InsertData();
        GetData();
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
        return (namespace + ":" + tableBaseName);
    }

    public String getTableBaseName() {
        return tableBaseName;
    }

    public void setTableBaseName(String tableBseName) {
        this.tableBaseName = tableBseName;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @SuppressWarnings("static-access")
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
