package com.ligadata.adapters.record;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import javax.sql.DataSource;

import org.easybatch.core.reader.RecordReader;
import org.easybatch.core.reader.RecordReaderClosingException;
import org.easybatch.core.reader.RecordReaderOpeningException;
import org.easybatch.core.reader.RecordReadingException;
import org.easybatch.core.record.Header;
import org.easybatch.core.record.Record;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
@RequiredArgsConstructor
@AllArgsConstructor
public class TableReader implements RecordReader{
	@Getter @Setter @NonNull
	private DataSource ds;
	@Getter @Setter @NonNull
	private String tableName;
	@Getter @Setter @NonNull
	private String columns;
	@Getter @Setter
	private String whereClause;
	
	@Getter @Setter
	private int fetchSize;
	@Getter @Setter
	private int queryTimeout;
	
	private Connection connection;
	private Statement statement;
	private ResultSet resultSet;
	private ResultSetMetaData resultSetMetaData;
	
	private long currentRecordNumber;
	private ArrayList<ColumnMetaInfo> metaMap;
	

	@Override
	public void close() throws RecordReaderClosingException {
		try{
			if(resultSet != null)resultSet.close();
			if(statement != null)statement.close();
			if(connection != null)connection.close();
		}catch(SQLException exc){
			log.error(exc.getMessage());
			throw new RecordReaderClosingException("Error while closing record reader", exc);
		}
	}

	@Override
	public String getDataSourceName() {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		try{
			if(connection == null)connection = ds.getConnection();
			sb.append("Conection URL - "+connection.getMetaData().getURL());
			sb.append("TableName - "+getTableName());
			sb.append("Columns - "+getColumns());
			if(getWhereClause()!= null && getWhereClause().length()>0)
				sb.append("WhereCkause - "+getWhereClause());
		}catch(SQLException exc){
			log.error(exc.getMessage());
		}
		return sb.toString();
	}

	@Override
	public Long getTotalRecords() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasNextRecord(){
		try{
			return resultSet.next();
		}catch(SQLException exc){
			exc.printStackTrace();
			log.error("Error while getting next record "+exc.getMessage());
			return false;
		}
	}

	@Override
	public void open() throws RecordReaderOpeningException {
		currentRecordNumber = 0;
		try{
			if(connection == null)connection = ds.getConnection();
			if(statement == null)statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			statement.setFetchSize(fetchSize);
			statement.setQueryTimeout(queryTimeout);
			StringBuilder query = new StringBuilder();
			query.append("SELECT "+getColumns());
			query.append(" FROM "+getTableName());
			if(getWhereClause()!=null && getWhereClause().length()>0)
				query.append(getWhereClause());
			resultSet = statement.executeQuery(query.toString());
			resultSetMetaData = resultSet.getMetaData();
			if(metaMap == null)
				metaMap = new ArrayList<ColumnMetaInfo>();
			else metaMap.clear();
			for(int j=1;j<=resultSetMetaData.getColumnCount();j++){
				metaMap.add(new ColumnMetaInfo(resultSetMetaData.getColumnName(j), resultSetMetaData.getColumnClassName(j), resultSetMetaData.getColumnType(j)));
			}
		}catch(SQLException exc){
			log.error(exc.getMessage());
			throw new RecordReaderOpeningException("Error while opening record reader", exc);
		}
	}

	@Override
	public Record readNextRecord() throws RecordReadingException {
		Header header = new Header(++currentRecordNumber, getDataSourceName(), new Date());
        //return new JdbcRecord(header, resultSet);
		
		HashMap<String, Object> dataMap = new HashMap();
		try{
			for(ColumnMetaInfo info:metaMap){
				dataMap.put(info.getColumnName(), resultSet.getObject(info.getColumnName()));
			}
		}catch(SQLException exc){
			exc.printStackTrace();
			throw new RecordReadingException("Unable to read record", exc);
		}
		
		JDBCMapRecord mapRecord = new JDBCMapRecord(header, dataMap);
		if(JDBCMapRecord.getFieldTypes() == null){
			JDBCMapRecord.setFieldTypes(metaMap);
		}
		return mapRecord;
	}

}
