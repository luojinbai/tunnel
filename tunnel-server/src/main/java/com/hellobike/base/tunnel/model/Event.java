package com.hellobike.base.tunnel.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import lombok.Getter;
import lombok.Setter;

/*
 * Copyright 2018 Shanghai Junzheng Network Technology Co.,Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain CONFIG_NAME copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @author machunxiao 2018-10-25
 */
@Setter
@Getter
public class Event implements Serializable {

	private static final long serialVersionUID = 3414755790085772526L;

	private long lsn;

	private transient String slotName;
	private transient String serverId;
	private String schema;
	private String table;
	private EventType eventType;
	private List<ColumnData> dataList = new ArrayList<>();

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Event event = (Event) o;
		return Objects.equals(schema, event.schema) && Objects.equals(table, event.table)
		        && eventType == event.eventType && Objects.equals(dataList, event.dataList);
	}

	@Override
	public int hashCode() {
		return Objects.hash(schema, table, eventType, dataList);
	}

	@Override
	public String toString() {
		return "Event{" + "schema='" + schema + '\'' + ", table='" + table + '\'' + ", eventType=" + eventType
		        + ", dataList=" + dataList + '}';
	}

}
