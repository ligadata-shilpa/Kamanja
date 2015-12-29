package com.ligadata.adapters.utility;



import javax.sql.DataSource;

import com.ligadata.adapters.pojo.TablePartitionInfo;

public class DBInputAdapter {
	/*
	 * Read DB Properties (Input)
	 * DB Admin User, DB Admin Pwd, DB Driver Class, DB Name
	 * Table, Where Clause
	 * Query
	 * Where Clause (only with table)
	 * Select Columns (only with table)
	 * Logic to Partition (partition column)
	 * Batch and Progress Reporting
	 * Chronological ?? (Single Thread)
	 * Executor - Partitions
	 * Multiple Nodes / Partitions ??
	 * Watcher Daemon Thread (Progress Reporting)
	 * Validation Components
	 * Direct Import and Export Like Sqoop
	 * 
	 * 
	 * Read Kafka Properties (Output)
	 * Read ZooKeeper Properties (Processing metadata)
	 * 
	 * 
	 */
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String dbUser = "kuser";
		String dbPwd = "centuser";
		String dbName = "kamanja";
		String dbDriver = "org.mariadb.jdbc.Driver";
		String dbURL = "jdbc:mariadb://192.168.1.11:3306";
		String tableName = "testTable";
		String columns = " emp_id, first_name, last_name, application_name, access_time, added_date";
		
		//String whereClause =" where added_date < current_date()";
		String whereClause ="";

		String query = "Select emp_id, first_name, last_name, application_name, access_time, added_date "
				+ " from testTable "
				+ " where added_date < current_date()";
		
		int numPartitions=8;
		String partCols = "emp_id";
		
		TablePartitionInfo partInfo = new TablePartitionInfo();
				
		if(DBValidator.validateConnectivity(dbUser, dbPwd, dbDriver, dbURL+"/"+dbName)){
			System.out.println("DB connection successful");
			DataSource ds = DBValidator.getDataSource(dbUser, dbPwd, dbDriver, dbURL+"/"+dbName);
			System.out.println("DataSource created successfully");
			partInfo.setDs(ds);
			
			if(DBValidator.validateTable(ds, tableName, columns, whereClause)){
				System.out.println("Valid Table and params....");
				partInfo.setColumns(columns);
				partInfo.setTableName(tableName);
				partInfo.setWhereClause(whereClause);
			}
			
			if(DBValidator.validateTablePartitionCol(ds, tableName, partCols)){
				System.out.println("Validated table partition column....");
				partInfo.setPartitionColumnName(partCols);
				partInfo.setPartitions(numPartitions);
				partInfo.fixPartInfo();
				System.out.println(partInfo.toString());
			}
			
			//Check the query Part here
			if(DBValidator.validateQuery(ds, query)){
				System.out.println("Valid query....");
			}
		}
	}

}
