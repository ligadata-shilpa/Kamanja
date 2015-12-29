package com.ligadata.adapters.pojo;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@ToString(includeFieldNames=true)
@Slf4j
public class TablePartitionInfo {
	@Getter @Setter
	private String tableName;
	@Getter @Setter
	private String partitionColumnName;
	@Getter @Setter
	private int partitions;
	@Getter @Setter
	private long min, max;
	@Getter @Setter
	private DataSource ds;
	@Getter @Setter
	private String whereClause;
	@Getter @Setter
	private String columns;
	
	@Getter
	private Map<Integer, String> partMap;
	private static final int defaultPartitions = 4;
	private long pcs;
	
	
	public void fixPartInfo(){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		try{
			con = ds.getConnection();
			stmt = con.createStatement();
			if(whereClause != null && whereClause.length()>0)
				rs = stmt.executeQuery("Select max("+partitionColumnName +") as max_value, min("+partitionColumnName+") as min_value from "+ tableName+" "+whereClause);
			else
				rs = stmt.executeQuery("Select max("+partitionColumnName +") as max_value, min("+partitionColumnName+") as min_value from "+ tableName);
			if( rs != null && rs.next()){
				min = rs.getInt("min_value");
				max = rs.getInt("max_value");
			}
			if(partMap == null){
				partMap = new HashMap<Integer, String>();
				int parts = (partitions > 0 ) ? partitions : defaultPartitions;
				pcs = Math.round((max-min)/parts);
				for(int j=1; j<=parts; j++){
					if(j==1)
						partMap.put((j), "( "+partitionColumnName+" >= "+min+" and "+partitionColumnName+" < "+(min+pcs)+" )");
					else if (j!= parts)
						partMap.put((j), "( "+partitionColumnName+" >= "+(min+((j-1)*pcs))+" and "+partitionColumnName+" < "+(min+((j)* pcs)) +" )");
					else
						partMap.put((j), "( "+partitionColumnName+" >= "+(min+((j-1)*pcs))+" and "+partitionColumnName+" <= "+max+" )");
				}
			}
			rs.close();
			rs = null;
			stmt.close();
			stmt = null;
			con.close();
			con = null;
		}catch(SQLException exc){
			log.error("TablePartitionInfo : Error "+exc.getMessage());
		}finally{
			try{
				if(rs != null) rs.close();
				if(stmt != null) stmt.close();
				if(con != null) con.close();
			}catch(SQLException exc){
				log.error("TablePartitionInfo : Error "+exc.getMessage());
			}
		}
	}
}
