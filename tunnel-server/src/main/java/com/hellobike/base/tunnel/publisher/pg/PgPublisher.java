/**
 * Project Name:tunnel-server
 * File Name:PgPublisher.java
 * Package Name:com.hellobike.base.tunnel.publisher.pg
 * Date:2019年6月6日下午5:19:15
 * Copyright (c) 2019, www.windo-soft.com All Rights Reserved.
 *
*/

package com.hellobike.base.tunnel.publisher.pg;

import static com.hellobike.base.tunnel.utils.TimeUtils.sleepOneSecond;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.hellobike.base.tunnel.config.PgConfig;
import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.model.InvokeContext;
import com.hellobike.base.tunnel.publisher.BasePublisher;
import com.hellobike.base.tunnel.publisher.IPublisher;
import com.hellobike.base.tunnel.publisher.es.EsPublisher;
import com.hellobike.base.tunnel.spi.api.CollectionUtils;
import com.hellobike.base.tunnel.utils.NamedThreadFactory;

import lombok.Getter;
import lombok.Setter;

/**
 * ClassName:PgPublisher <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason:	 TODO ADD REASON. <br/>
 * Date:     2019年6月6日 下午5:19:15 <br/>
 * @author   yibai
 * @version  
 * @since    JDK 1.6
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

//		this.restClients = new RestHighLevelClient[total];
//		for (int i = 0; i < total; i++) {
//			this.restClients[i] = newRestEsHighLevelClient();
//		}
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
		Event event = context.getEvent();
		System.out.println(pgConfigs);
		System.out.println("222: " + context);
		this.pgConfigs.forEach(pgConfigs -> internalPublish(context, callback, pgConfigs));
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
			Map<Object, List<Helper>> data = helpers.stream()
					.collect(Collectors.groupingBy(helper -> helper.pgConfig.getTable()));
			System.out.println(data);
			for (List<Helper> list : data.values()) {
				if (list.isEmpty()) {
					continue;
				}
//				syncSend(restClients[idx], toRequests(list));
			}
		} finally {
//			if (!helpers.isEmpty()) {
//				Map<String, Long> data = getMonitorData(helpers);
//				mapToStatics(data).forEach(statics -> TunnelMonitorFactory.getTunnelMonitor().collect(statics));
//			}
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
//		Arrays.stream(this.restClients).forEach(this::closeClosable);
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

	private Map<String, Object> executeQuery(String sql, InvokeContext context) {
		Connection connection = null;
		try {
			connection = getConnection(context);
			QueryRunner qr = new QueryRunner();
			Map<String, Object> query = qr.query(connection, sql, new MapHandler());
			if (query == null || query.isEmpty()) {
				query = new LinkedHashMap<>();
				LOG.warn("Select Nothing By SQL:{}", sql);
			}
			return query;
		} catch (Exception e) {
			//
		} finally {
			closeClosable(connection);
		}
		return new LinkedHashMap<>();
	}

	private Connection getConnection(InvokeContext ctx) throws SQLException {
		DruidDataSource dataSource = dataSources.get(ctx.getSlotName());
		if (dataSource == null) {
			DruidDataSource tmp;
			synchronized (this) {
				tmp = createDataSource(ctx);
			}
			dataSources.put(ctx.getSlotName(), tmp);
			dataSource = dataSources.get(ctx.getSlotName());
			LOG.info("DataSource Initialized. Slot:{},DataSource:{}", ctx.getSlotName(), dataSource.getName());
		}
		return dataSource.getConnection();
	}

	private DruidDataSource createDataSource(InvokeContext ctx) {
		DruidDataSource dataSource = new DruidDataSource();
		dataSource.setUsername(ctx.getJdbcUser());
		dataSource.setUrl(ctx.getJdbcUrl());
		dataSource.setPassword(ctx.getJdbcPass());
		dataSource.setValidationQuery("select 1");
		dataSource.setMinIdle(20);
		dataSource.setMaxActive(50);
		return dataSource;
	}

	@Setter
	@Getter
	private static class Helper {

		final PgConfig pgConfig;
		final InvokeContext context;

		private Helper(PgConfig pgConfig, InvokeContext context) {
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
