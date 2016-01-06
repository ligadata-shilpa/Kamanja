package com.ligadata.adapters.utility;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.StringTokenizer;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBValidator {
	final static Logger logger = LoggerFactory.getLogger(DBValidator.class);
	
	public static boolean validateConnectivity(String user, String pwd, String driver, String jdbcURL){
		boolean valid = false;
		if((user != null && user.length() > 0) && (pwd!= null && pwd.length() > 0)){
			if(jdbcURL != null && jdbcURL.length() > 0){
				if(driver != null && driver.length() > 0){
					try {
						Class.forName(driver);
						Connection con = DriverManager.getConnection(jdbcURL, user, pwd);
						if(con != null){
							valid = true;
							con.close();
						}
					} catch (ClassNotFoundException exc) {
						// TODO Auto-generated catch block
						logger.error("DB Connectivity - Unable to load the Driver Class ");
					}catch(SQLException exc){
						exc.printStackTrace();
						logger.error("Db Connectivity "+exc.getMessage());
					}
				}else{
					logger.error("DB Connectivity - JDBC Driver is necessary for DB Connectivity");
				}
				
			}else{
				logger.error("DB Connectivity - JDBC URL is necessary for DB Connectivity");
			}
		}else{
			if(user==null || user.length()==0){
				logger.error("DB Credentials Missing - User ID is necessary for DB Connectivity");
			}
			if(pwd==null || pwd.length()==0){
				logger.error("Db Credentials Missing - User password is necessary for DB Connectivity");
			}
		}
		return valid;
	}	
	
	//Return a pooled datasource 
	public static BasicDataSource getDataSource(String user, String pwd, String driver, String jdbcURL){
		BasicDataSource ds = new BasicDataSource();
		
		ds.setDriverClassName(driver);
		ds.setUrl(jdbcURL);
		ds.setUsername(user);
		ds.setPassword(pwd);
		
		
		ds.setTestWhileIdle(false);
		ds.setTestOnBorrow(true);
		ds.setValidationQuery("Select 1");
		ds.setTestOnReturn(false);
		
		ds.setMaxTotal(100);
		ds.setMaxIdle(5);
		ds.setMinIdle(0);
		ds.setInitialSize(5);
		ds.setMaxWaitMillis(5000);
		
		return ds;
	}
	
	public static boolean validateQuery(DataSource ds, String query){
		boolean valid = false;
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		if(query != null && query.length()> 0 ){
			try {
				conn = ds.getConnection();
				stmt = conn.createStatement();
				stmt.execute(query +" LIMIT 1");
				rs = stmt.getResultSet();
				if(rs.next()){
					valid = true;
				}
				rs.close();
				rs = null;
				stmt.close();
				stmt = null;
				conn.close();
				conn = null;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error("DB Validator : SQL Message "+e.getMessage());
			}finally{
				try{
					if(rs != null) rs.close();
					if(stmt != null )stmt.close();
					if(conn != null )conn.close();
				}catch(SQLException exc){
					logger.error("DB Validator : SQL Message "+exc.getMessage());
				}
			}
		}else{
			logger.error("DB Validator : Unable to execute query, empty string.");
		}
		return valid;
	}
	
	public static boolean validateTable(DataSource ds, String table, String columns, String where){
		boolean valid = false;
		Connection conn = null;
		DatabaseMetaData dbmd = null;
		ResultSet rs= null, rs1 = null;
		boolean tableExists = false, columnsExist = true; 
		if( (table != null && table.length() > 0) && (columns != null && columns.length()> 0)){
			try{
				conn = ds.getConnection();
				dbmd = conn.getMetaData();
				rs = dbmd.getTables(null, null, table, new String[]{"TABLE"});
				//First check if the table exists
				if(rs != null && rs.next()){
					tableExists = true;
					//Check if the columns belong to the table
					StringTokenizer st = new StringTokenizer(columns, ",");
					while(st.hasMoreTokens()){
						String colName = st.nextToken().trim();
						rs1 = dbmd.getColumns(null, null, table, colName);
						if(rs1 != null && rs1.next()){
							//Column exists - do nothing
						}else{
							columnsExist = false;
						}
						if(!columnsExist)break;
					}
				}else{
					logger.error("DB Validator : Table "+table+" doesnot exist");
				}
				//Now build the query and validate it
				if(tableExists && columnsExist){
					String query = null;
					if (where != null && where.length() > 0)
						query = "Select "+ columns +" from "+ table +" "+ where;
					else
						query = "Select "+ columns +" from "+ table;
					
					//Validate the query now
					if( validateQuery(ds, query)){
						valid = true;
					}
				}
				rs1.close();
				rs1 = null;
				rs.close();
				rs = null;
				conn.close();
				conn = null;
			}catch(SQLException exc){
				logger.error("DB Validator : SQL Message "+exc.getMessage());
			}finally{
				try{
					if(rs1 != null) rs1.close();
					if(rs != null) rs.close();
					if(conn != null )conn.close();
				}catch(SQLException exc){
					logger.error("DB Validator : SQL Message "+exc.getMessage());
				}
			}
		}else{
			if(table == null || table.length()==0){
				logger.error("DB Validator : Table name is empty");
			}
			if(columns == null || columns.length()==0){
				logger.error("DB Validator : Table Columns are empty");
			}
		}
		return valid;
	}
	
	public static boolean validateTablePartitionCol(DataSource ds, String table, String partColName){
		boolean valid = false;
		Connection conn = null;
		DatabaseMetaData dbmd = null;
		ResultSet rs= null, rs1 = null;
		try{
			conn = ds.getConnection();
			dbmd = conn.getMetaData();
			rs = dbmd.getTables(null, null, table, new String[]{"TABLE"});
			
			//First check if the table exists
			if(rs != null && rs.next()){	
				rs1 = dbmd.getColumns(null, null, table, partColName.trim());
				if(rs1 != null && rs1.next()){
					if(rs1.getInt("DATA_TYPE") == java.sql.Types.BIGINT || rs1.getInt("DATA_TYPE") == java.sql.Types.NUMERIC  ||
							rs1.getInt("DATA_TYPE") == java.sql.Types.INTEGER  || rs1.getInt("DATA_TYPE") == java.sql.Types.INTEGER ){
						valid = true;	
					}else{
						logger.error("DB Validator : Partition Column should be numeric");
					}
				}
			}
			rs.close();
			rs = null;
			conn.close();
			conn = null;
		}catch(SQLException exc){
			logger.error("DB Validator : SQL Message "+exc.getMessage());
		}finally{
			try{
				if(rs1 != null) rs1.close();
				if(rs != null) rs.close();
				if(conn != null )conn.close();
			}catch(SQLException exc){
				logger.error("DB Validator : SQL Message "+exc.getMessage());
			}
		}
		return valid;
	}

}
