/**
 * Project Name:tunnel-server
 * File Name:PgPublisher.java
 * Package Name:com.hellobike.base.tunnel.publisher.pg
 * Date:2019骞�6鏈�6鏃ヤ笅鍗�5:19:15
 * Copyright (c) 2019, www.windo-soft.com All Rights Reserved.
 *
*/

package com.hellobike.base.tunnel.publisher.pg;

import static com.hellobike.base.tunnel.utils.TimeUtils.sleepOneSecond;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.hellobike.base.tunnel.config.PgConfig;
import com.hellobike.base.tunnel.model.ColumnData;
import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.model.EventType;
import com.hellobike.base.tunnel.model.InvokeContext;
import com.hellobike.base.tunnel.publisher.BasePublisher;
import com.hellobike.base.tunnel.publisher.IPublisher;
import com.hellobike.base.tunnel.publisher.es.EsPublisher;
import com.hellobike.base.tunnel.spi.api.CollectionUtils;
import com.hellobike.base.tunnel.utils.NamedThreadFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * ClassName:PgPublisher <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2019骞�6鏈�6鏃� 涓嬪崍5:19:15 <br/>
 * 
 * @author yibai
 * @version
 * @since JDK 1.6
 * @see
 */
public class PgPublisher extends BasePublisher implements IPublisher {

	private static final Logger /**/ LOG = LoggerFactory.getLogger(EsPublisher.class);
	private static final int /**/ MAX_CACHED = 10240;

	private final List<PgConfig> /**/ pgConfigs;
	private final ThreadPoolExecutor /**/ executor;
	private final LinkedBlockingQueue<Helper> /**/ requestHelperQueue;
	private final Map<String, DruidDataSource> /**/ dataSources;

	private volatile boolean /**/ started;

	public PgPublisher(List<PgConfig> pgConfigs) {
		this.pgConfigs = pgConfigs;
		int total = 8;
		this.executor = new ThreadPoolExecutor(total, total, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(5000),
		        new NamedThreadFactory("PgSendThread"));

		// this.restClients = new RestHighLevelClient[total];
		// for (int i = 0; i < total; i++) {
		// this.restClients[i] = newRestEsHighLevelClient();
		// }
		this.dataSources = new ConcurrentHashMap<>();
		this.requestHelperQueue = new LinkedBlockingQueue<>(81920);

		started = true;
		for (int i = 0; i < total; i++) {
			this.executor.submit(new Sender(i));
		}

	}

	@Override
	public void publish(Event event, Callback callback) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void publish(InvokeContext context, Callback callback) {
		this.pgConfigs.forEach(pgConfig -> internalPublish(context, callback, pgConfig));
	}

	private void internalPublish(InvokeContext context, Callback callback, PgConfig pgConfig) {
		if (CollectionUtils.isEmpty(pgConfig.getFilters())
		        || pgConfig.getFilters().stream().allMatch(filter -> filter.filter(context.getEvent()))) {
			sendToEs(pgConfig, context, callback);
		}
	}

	private void sendToEs(PgConfig pgConfig, InvokeContext context, Callback callback) {
		try {
			requestHelperQueue.put(new Helper(pgConfig, context));
			onSuccess(callback);
		} catch (Exception e) {
			//
			LOG.error("Put Data To MemQueue Failure", e);
			onFailure(callback, e);
		}
		if (requestHelperQueue.size() >= MAX_CACHED) {
			forceFlushMemQueue(0);
		}
	}

	private void forceFlushMemQueue(int idx) {
		List<Helper> helpers = pollHelperFromQueue(requestHelperQueue);
		try {
			if (CollectionUtils.isEmpty(helpers)) {
				return;
			}
			toRequests(helpers);
		} finally {
			// if (!helpers.isEmpty()) {
			// Map<String, Long> data = getMonitorData(helpers);
			// mapToStatics(data).forEach(statics ->
			// TunnelMonitorFactory.getTunnelMonitor().collect(statics));
			// }
		}
	}

	private void toRequests(List<Helper> helpers) {
		for (Helper helper : helpers) {
			// System.err.println(JSON.toJSONString(helper));
			// Map<String, String> values =
			// helper.context.getEvent().getDataList().stream()
			// .collect(Collectors.toMap(ColumnData::getName,
			// ColumnData::getValue));
			// List<String> args = new ArrayList<>();
			// for (String k : parameters) {
			// args.add(getValue(values.get(k)));
			// }
			// System.out.println(values); // {data=12, id=29}
			PgConfig pgConfig = helper.getPgConfig();
			String sql = buildSQL(helper);
			execute(sql, pgConfig);
		}
	}

	public static void main(String[] args) {
		// String text = "";
		// Helper helper = JSON.parseObject(text, Helper.class);
		// System.out.println(helper);
		// Event event = helper.getContext().getEvent();
		// System.out.println(event);
		// for (ColumnData columnData : event.getDataList()) {
		// System.out.println(columnData);
		// }

		StringBuilder buf = new StringBuilder();
		buf.append("12323");
		System.out.println(tryDeleteChar(buf, ','));

		System.out.println(tryDeleteString(buf, "23"));

	}

	private String buildSQL(Helper helper) {
		String sql = "";
		Event event = helper.getContext().getEvent();
		// System.out.println(event);
		// for (ColumnData columnData : event.getDataList()) {
		// System.out.println(columnData);
		// }
		PgConfig pgConfig = helper.getPgConfig();
		EventType eventType = event.getEventType();
		List<ColumnData> dataList = event.getDataList();
		List<String> pks = pgConfig.getPks(); // 涓婚敭鏀緒here鏉′欢

		switch (eventType) {
		case INSERT:
			StringBuilder columnBuf = new StringBuilder(100);
			StringBuilder valuesBuf = new StringBuilder(100);
			columnBuf.append("INSERT INTO ").append(pgConfig.getTable()).append("(");
			valuesBuf.append("VALUES(");
			for (ColumnData columnData : dataList) {
				columnBuf.append(columnData.getName()).append(",");
				valuesBuf.append(columnData.buildData()).append(",");
			}
			tryDeleteChar(columnBuf, ',');
			tryDeleteChar(valuesBuf, ',');
			columnBuf.append(") ");
			valuesBuf.append(")");
			sql = columnBuf.toString() + valuesBuf.toString();
			break;
		case UPDATE:
			StringBuilder setBuf = new StringBuilder(100);
			StringBuilder whereBuf = new StringBuilder(100);
			setBuf.append("UPDATE ").append(pgConfig.getTable()).append(" SET ");
			whereBuf.append(" WHERE ");
			for (ColumnData columnData : dataList) {
				String colname = columnData.getName();
				if (pks.contains(colname)) {
					whereBuf.append(colname).append("=").append(columnData.buildData()).append("AND ");
				} else {
					setBuf.append(colname).append("=").append(columnData.buildData()).append(",");
				}
			}
			tryDeleteString(whereBuf, "AND ");
			tryDeleteChar(setBuf, ',');
			sql = setBuf.toString() + whereBuf.toString();
			break;
		case DELETE:
			StringBuilder buf = new StringBuilder(100);
			buf.append("DELETE FROM ").append(pgConfig.getTable()).append(" WHERE ");
			for (ColumnData columnData : dataList) {
				String colname = columnData.getName();
				if (pks.contains(colname)) {
					buf.append(colname).append("=").append(columnData.buildData()).append("AND ");
				} else {
				}
			}
			tryDeleteString(buf, "AND ");
			sql = buf.toString();
			break;
		default:
			throw new IllegalArgumentException(String.format("不支持的事件类型： %s", eventType));
		}
		System.err.println("执行SQL: " + sql);
		LOG.debug("执行SQL: {}", sql);
		return sql;
	}

	private static StringBuilder tryDeleteChar(StringBuilder buf, char delete) {
		if (buf.charAt(buf.length() - 1) == delete) {
			buf.deleteCharAt(buf.length() - 1);
		}
		return buf;
	}

	private static StringBuilder tryDeleteString(StringBuilder buf, String delete) {
		if (buf.toString().endsWith(delete)) {
			buf.delete(buf.length() - delete.length(), buf.length());
		}
		return buf;
	}

	private void execute(String sql, PgConfig pgConfig) {
		try (Connection conn = getConnection(pgConfig)) {
			QueryRunner qr = new QueryRunner();
			int execute = qr.execute(conn, sql);
			System.err.println("SQL执行结果： " + execute);
		} catch (Exception e) {
			//
			LOG.error("SQL执行异常", e);
		}
	}

	private List<Helper> pollHelperFromQueue(BlockingQueue<Helper> queue) {
		int len = queue.size();
		int capacity = Math.min(MAX_CACHED / 2, len);
		List<Helper> helpers = new ArrayList<>(capacity);
		queue.drainTo(helpers, capacity);
		return helpers;
	}

	@Override
	public void close() {
		this.started = false;
		this.executor.shutdown();
		// Arrays.stream(this.restClients).forEach(this::closeClosable);
		this.dataSources.values().forEach(this::closeClosable);
		this.dataSources.clear();
		LOG.info("EsPublisher Closed");
	}

	private void closeClosable(AutoCloseable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception e) {
				//
			}
		}
	}

	private Connection getConnection(PgConfig pgConfig) throws SQLException {
		DruidDataSource dataSource = dataSources.get(pgConfig.getUrl());
		if (dataSource == null) {
			DruidDataSource tmp;
			synchronized (this) {
				tmp = createDataSource(pgConfig);
			}
			dataSources.put(pgConfig.getUrl(), tmp);
			dataSource = dataSources.get(pgConfig.getUrl());
			LOG.info("DataSource Initialized. url:{},DataSource:{}", pgConfig.getUrl(), dataSource.getName());
		}
		return dataSource.getConnection();
	}

	private DruidDataSource createDataSource(PgConfig pgConfig) {
		DruidDataSource dataSource = new DruidDataSource();
		dataSource.setUsername(pgConfig.getUsername());
		dataSource.setUrl(pgConfig.getUrl());
		dataSource.setPassword(pgConfig.getPassword());
		dataSource.setValidationQuery("select 1");
		dataSource.setMinIdle(10);
		dataSource.setMaxActive(30);
		return dataSource;
	}

	@Setter
	@Getter
	@ToString
	private static class Helper {

		private PgConfig pgConfig;
		private InvokeContext context;

		@SuppressWarnings("unused")
		public Helper() {
		}

		public Helper(PgConfig pgConfig, InvokeContext context) {
			this.pgConfig = pgConfig;
			this.context = context;
		}
	}

	private class Sender implements Runnable {

		private final int idx;

		public Sender(int idx) {
			this.idx = idx;
		}

		@Override
		public void run() {
			while (started) {
				long s = System.currentTimeMillis();
				try {
					forceFlushMemQueue(this.idx);
				} catch (Exception e) {
					LOG.warn("flush data to es failure", e);
				} finally {
					sleepOneSecond(s, System.currentTimeMillis());
				}
			}
		}
	}

}
