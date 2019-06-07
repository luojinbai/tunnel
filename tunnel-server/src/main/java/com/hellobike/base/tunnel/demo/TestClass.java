/**
 * Project Name:tunnel-server
 * File Name:TestClass.java
 * Package Name:com.hellobike.base.tunnel.demo
 * Date:2019年6月6日下午2:40:50
 * Copyright (c) 2019, www.windo-soft.com All Rights Reserved.
 *
*/

package com.hellobike.base.tunnel.demo;

import org.apache.commons.lang3.StringUtils;

/**
 * ClassName:TestClass <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason:	 TODO ADD REASON. <br/>
 * Date:     2019年6月6日 下午2:40:50 <br/>
 * @author   yibai
 * @version  
 * @since    JDK 1.6
 * @see 	 
 */
public class TestClass {

	public static void main(String[] args) {

		System.out.println(StringUtils.isAnyBlank(" ", "", "123"));
		System.out.println(StringUtils.isAnyEmpty(" ", " ", "123"));

	}

}
