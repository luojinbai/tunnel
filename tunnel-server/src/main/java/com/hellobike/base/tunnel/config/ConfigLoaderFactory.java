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
package com.hellobike.base.tunnel.config;

import com.hellobike.base.tunnel.apollo.ApolloConfigLoader;
import com.hellobike.base.tunnel.config.file.FileConfigLoader;

/**
 * @author machunxiao create at 2018-12-27
 */
public class ConfigLoaderFactory {

	private ConfigLoaderFactory() {
	}

	public static ConfigLoader getConfigLoader(TunnelConfig tunnelConfig) {
		if (tunnelConfig.isUseApollo()) {
			return new ApolloConfigLoader(tunnelConfig.getAppId(), tunnelConfig.getMetaDomain());
		}
		/**
		 * tunnel_subscribe_config={"pg_dump_path":"","subscribes":[{"slotName":"slot_for_test","pgConnConf":{"host":"localhost","port":5432,"database":"test1","user":"test1","password":"test1"},"rules":[{"table":"t_department_info","fields":null,"pks":["id"],"esid":["id"],"index":"t_department_info","type":"logs"}],"esConf":{"addrs":"http://localhost:9200"}}]}
		 * tunnel_zookeeper_address=localhost:2181
		 */
		return new FileConfigLoader(tunnelConfig.getConfigFile());
	}

}
