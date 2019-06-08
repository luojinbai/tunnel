package com.hellobike.base.tunnel.model;

import com.hellobike.base.tunnel.publisher.IPublisher;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

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
 * @author machunxiao create at 2018-12-14
 */
@ToString
@Setter
@Getter
public class InvokeContext {

	private String serverId;
	private String slotName;

	private String jdbcUrl;
	private String jdbcUser;
	private String jdbcPass;

	private long lsn;
	private String message;

	private String xid;
	private Event event;

	private transient IPublisher.Callback callback;

}
