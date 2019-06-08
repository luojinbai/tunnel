package com.hellobike.base.tunnel.model;

import org.apache.commons.lang3.StringUtils;

import lombok.AllArgsConstructor;

/**
 * @author yibai
 *
 */
@AllArgsConstructor
public enum DataType {

	/***/
	TYPE_INT(EDataBuilder.common),
	/***/
	TYPE_TIMESTAMP(EDataBuilder.quotes),
	/***/
	TYPE_VARCHAR(EDataBuilder.quotes),
	/***/
	TYPE_DATE(EDataBuilder.quotes),
	/***/
	TYPE_TEXT(EDataBuilder.quotes);

	EDataBuilder dataBuilder;

	public String buildData(String data) {
		return dataBuilder.buildData(data);
	}

	/**
	 * 字符串转字段类型
	 * 
	 * @param dataType
	 * @return
	 */
	public static DataType typeValueOf(String dataType) {
		if (StringUtils.isBlank(dataType)) {
			throw new IllegalArgumentException("value cannot be null");
		}
		switch (dataType) {
		case "text":
			return TYPE_TEXT;
		case "integer":
			return TYPE_INT;
		case "character varying":
			return TYPE_VARCHAR;
		case "timestamp without time zone":
		case "timestamp with time zone":
			return TYPE_TIMESTAMP;
		case "date":
			return TYPE_DATE;
		default:
			break;
		}
		throw new IllegalArgumentException(String.format("还没有支持该类型: %s", dataType));

	}

	public static enum EDataBuilder {
		common() {
			@Override
			public String buildData(String data) {
				return data;
			}
		},
		quotes() {
			@Override
			public String buildData(String data) {
				return new StringBuilder(data.length() + 2).append("'").append(data).append("'").toString();
			}
		};

		public abstract String buildData(String data);

	}

}
