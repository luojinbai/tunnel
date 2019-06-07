/**
 * Project Name:tunnel-server
 * File Name:PgConfig.java
 * Package Name:com.hellobike.base.tunnel.config
 * Date:2019年6月6日下午1:57:28
 * Copyright (c) 2019, www.windo-soft.com All Rights Reserved.
 *
*/

package com.hellobike.base.tunnel.config;

import java.util.ArrayList;
import java.util.List;

import com.hellobike.base.tunnel.filter.IEventFilter;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * ClassName:PgConfig <br/>
 * Function: 目标库配置. <br/>
 * Date:     2019年6月6日 下午1:57:28 <br/>
 * @author   yibai
 * @version  
 * @since    JDK 1.6
 * @see 	 
 */
@Setter
@Getter
@ToString
public class PgConfig {
	private String url;
	private String username;
	private String password;
	private String table;
	private List<IEventFilter> filters = new ArrayList<>();
}
