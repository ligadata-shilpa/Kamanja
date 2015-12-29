package com.ligadata.adapters.record;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import javax.sql.DataSource;

import org.easybatch.core.reader.RecordReader;
import org.easybatch.core.reader.RecordReaderClosingException;
import org.easybatch.core.reader.RecordReaderOpeningException;
import org.easybatch.core.reader.RecordReadingException;
import org.easybatch.core.record.Header;
import org.easybatch.core.record.Record;
import org.easybatch.jdbc.JdbcRecord;

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
public class QueryReader implements RecordReader{
	@Getter @Setter @NonNull
	private DataSource ds;
	@Getter @Setter @NonNull
	private String query;
	
	@Getter @Setter
	private int fetchSize;
	@Getter @Setter
	private int queryTimeout;
	
	private Connection connection;
	private Statement statement;
	private ResultSet resultset;
	
	private long currentRecordNumber;
	

	@Override
	public void close() throws RecordReaderClosingException {
		try{
			if(resultset != null)resultset.close();
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
			sb.append("TableName - "+getQuery());
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
			return resultset.next();
		}catch(SQLException exc){
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
			statement.setFetchDirection(fetchSize);
			statement.setQueryTimeout(queryTimeout);
			resultset = statement.executeQuery(query);
		}catch(SQLException exc){
			log.error(exc.getMessage());
			throw new RecordReaderOpeningException("Error while opening record reader", exc);
		}
	}

	@Override
	public Record readNextRecord() throws RecordReadingException {
		Header header = new Header(++currentRecordNumber, getDataSourceName(), new Date());
        return new JdbcRecord(header, resultset);
	}
}
