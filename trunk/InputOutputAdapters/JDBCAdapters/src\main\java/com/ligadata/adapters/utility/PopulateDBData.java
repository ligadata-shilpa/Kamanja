package com.ligadata.adapters.utility;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class PopulateDBData {
	
	public static void main(String args[]){
		String dbUser = "root";
		String dbPwd = "centuser";
		String dbName = "kamanja";
		String dbDriver = "org.mariadb.jdbc.Driver";
		String dbURL = "jdbc:mariadb://192.168.1.9:3306";
		String tableName = "testTable";
		
		String tableScript = "CREATE TABLE IF NOT EXISTS "+tableName
				+ " ( emp_id bigint auto_increment primary key,"
				+ "first_name varchar(256) ,"
				+ "last_name varchar(256) ,"
				+ "application_name varchar(256),"
				+ "access_time datetime,"
				+ "added_date date"
				+ ") engine=InnoDB default charset latin1";
		boolean tableExists = false;
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			Class.forName(dbDriver);
			
			con = DriverManager.getConnection(dbURL+"/"+dbName, dbUser, dbPwd);
			
			if(con!=null){
				System.out.println("Connected to the DB");
				DatabaseMetaData dbmd = con.getMetaData();
				rs = dbmd.getTables(null, null, "%", null);
				
				//Look up for the tableName
				while(rs!=null && rs.next()){
					if(rs.getString(3).equalsIgnoreCase(tableName)){
						tableExists = true;
						break;
					}
				}
				
				//Create Table if it doesnt exist
				if(!tableExists){
					Statement stmt = con.createStatement();
					stmt.execute(tableScript);
					if(stmt.getUpdateCount() == 0 ){
						System.out.println("Created table "+tableName+" successfully...");
					}
				}else{
					System.out.println("Table "+tableName+" exists, skiping creation processs...");
				}
				
				pstmt = con.prepareStatement("INSERT INTO "+tableName+" (first_name, last_name, application_name, access_time, added_date  ) "
						+ "VALUES (?, ? , ? , CURRENT_TIMESTAMP , CURRENT_DATE ) ");
				con.setAutoCommit(false);
				//Now pump in 50k Records
				Random randomGenerator = new Random();
				for(int j=0;j<100000; j++){
					int randomName = randomGenerator.nextInt(100);
					int randomApp = randomGenerator.nextInt(50);
					pstmt.setString(1, "fname"+randomName);
					pstmt.setString(2, "lname"+randomName);
					pstmt.setString(3, "app"+randomApp);
					int rows  = pstmt.executeUpdate();
					
					if(j % 100 == 0)
						System.out.println("Inserting record "+j);
					
					if(rows == 1)
						continue;
					else
						break;
				}
				con.commit();
				
				rs.close();
				rs = null;
				pstmt.close();
				pstmt = null;
				con.close();
				con = null;
				
			}else{
				System.out.println("Unable to connect to the DB");
			}
		}catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try{
				if(rs != null) rs.close();
				if(pstmt != null) pstmt.close();
				if(con != null) con.close();
			}catch(SQLException exc){
				exc.printStackTrace();
				
			}
		}
	}
}
