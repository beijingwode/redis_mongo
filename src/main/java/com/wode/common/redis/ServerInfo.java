package com.wode.common.redis;

/**
 * 
 * <pre>
 * 功能说明: 服务器信息
 * 日期:	2015年4月27日
 * 开发者:	宋艳垒
 * 
 * 历史记录
 *    修改内容：
 *    修改人员：
 *    修改日期： 2015年4月27日
 * </pre>
 */
public class ServerInfo {
	
	//服务器ip地址
	private String ip;
	//服务器redis端口号
	private int port;
	//服务器权重
	private int Weight;
	
	public int getWeight() {
		return Weight;
	}
	public void setWeight(int weight) {
		Weight = weight;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	
}
