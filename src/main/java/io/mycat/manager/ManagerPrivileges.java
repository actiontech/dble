/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.manager;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.druid.wall.WallCheckResult;
import com.alibaba.druid.wall.WallProvider;

import io.mycat.MycatServer;
import io.mycat.config.Alarms;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.FirewallConfig;
import io.mycat.config.model.UserConfig;
import io.mycat.config.model.UserPrivilegesConfig;
import io.mycat.config.MycatPrivileges;
import io.mycat.net.handler.FrontendPrivileges;
import io.mycat.route.RouteResultset;
import io.mycat.server.ServerConnection;

/**
 * @author mycat
 */
public class ManagerPrivileges extends MycatPrivileges {
	/**
	 * 无需每次建立连接都new实例。
	 */
    private static ManagerPrivileges instance = new ManagerPrivileges();
	
    private static final Logger ALARM = LoggerFactory.getLogger("alarm");
    
    private static boolean check = false;	
    private final static ThreadLocal<WallProvider> contextLocal = new ThreadLocal<WallProvider>();

    public static ManagerPrivileges instance() {
    	return instance;
    }
    
    private ManagerPrivileges() {
    	super();
    }
    
    @Override
    public boolean checkFirewallWhiteHostPolicy(String user, String host) {
		
	MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        FirewallConfig firewallConfig = mycatConfig.getFirewall();
	UserConfig rUser = mycatConfig.getUsers().get(user);
	
	if (rUser == null || rUser.isManager() == false)
	    	return false;
        
        //防火墙 白名单处理
        boolean isPassed = false;
        
        Map<String, List<UserConfig>> whitehost = firewallConfig.getWhitehost();
        if (whitehost == null || whitehost.size() == 0) {        	
        	Map<String, UserConfig> users = mycatConfig.getUsers();
        	isPassed = users.containsKey(user);
        	
        } else {        	
        	List<UserConfig> list = whitehost.get(host);
			if (list != null) {			
				for (UserConfig userConfig : list) {
					if (userConfig.getName().equals(user)) {
						isPassed = true;
						break;
					}
				}
			}        	
        }
        
        if ( !isPassed ) {
        	 ALARM.error(new StringBuilder().append(Alarms.FIREWALL_ATTACK).append("[host=").append(host)
                     .append(",user=").append(user).append(']').toString());
        	 return false;
        }        
        return true;
    }
}
