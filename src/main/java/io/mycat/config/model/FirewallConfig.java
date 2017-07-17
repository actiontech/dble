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
package io.mycat.config.model;

import com.alibaba.druid.wall.WallConfig;
import com.alibaba.druid.wall.WallProvider;
import com.alibaba.druid.wall.spi.MySqlWallProvider;
import io.mycat.MycatServer;
import io.mycat.config.MycatConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 防火墙配置定义
 * 
 * @author songwie
 * @author zhuam
 */
public final class FirewallConfig {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(FirewallConfig.class);
	
    private Map<String, List<UserConfig>> whitehost;
    private List<String> blacklist;
    private boolean check = false;
    
    private WallConfig wallConfig = new WallConfig();
     
    private static WallProvider provider ;
    
    public FirewallConfig() { }
    
    public void init(){
    	if(check){
    		provider = new MySqlWallProvider(wallConfig);
    		provider.setBlackListEnable(true);
    	}
    }
    
    public WallProvider getWallProvider(){
    	return provider;
    }

	public Map<String, List<UserConfig>> getWhitehost() {
		return this.whitehost;
	}
	public void setWhitehost(Map<String, List<UserConfig>> whitehost) {
		this.whitehost = whitehost;
	}
	
	public boolean addWhitehost(String host, List<UserConfig> Users) {
		if (existsHost(host)){
			return false;	
		}
		else {
		 this.whitehost.put(host, Users);
		 return true;
		}
	}
	
	public List<String> getBlacklist() {
		return this.blacklist;
	}
	public void setBlacklist(List<String> blacklist) {
		this.blacklist = blacklist;
	}
	
	public WallProvider getProvider() {
		return provider;
	}

	public boolean existsHost(String host) {
		return this.whitehost!=null && whitehost.get(host)!=null ;
	}
	public boolean canConnect(String host,String user) {
		if(whitehost==null || whitehost.size()==0){
			MycatConfig config = MycatServer.getInstance().getConfig();
			Map<String, UserConfig> users = config.getUsers();
			return users.containsKey(user);
		}else{
			List<UserConfig> list = whitehost.get(host);
			if(list==null){
				return false;
			}
			for(UserConfig userConfig : list){
				if(userConfig.getName().equals(user)){
					return true;
				}
			}
		}
		return false ;
	}
	
	public static void setProvider(WallProvider provider) {
		FirewallConfig.provider = provider;
	}

	public void setWallConfig(WallConfig wallConfig) {
		this.wallConfig = wallConfig;
		
	}

	public boolean isCheck() {
		return this.check;
	}

	public void setCheck(boolean check) {
		this.check = check;
	}

	public WallConfig getWallConfig() {
		return this.wallConfig;
	}

}