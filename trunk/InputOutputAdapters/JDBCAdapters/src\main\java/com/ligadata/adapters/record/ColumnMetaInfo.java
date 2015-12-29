package com.ligadata.adapters.record;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ColumnMetaInfo {
	@Getter @Setter
	String columnName;
	@Getter @Setter
	String javaType;
	@Getter @Setter
	int JDBCType;
}
