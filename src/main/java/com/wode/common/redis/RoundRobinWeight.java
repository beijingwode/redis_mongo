package com.wode.common.redis;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.wode.common.util.StringUtils;

import redis.clients.jedis.Jedis;

/**
 * 
 * <pre>
 * 功能说明: 根据主机权重负载均衡
 * 日期:	2015年4月27日
 * 开发者:	宋艳垒
 * 
 * 历史记录
 *    修改内容：
 *    修改人员：
 *    修改日期： 2015年4月27日
 * </pre>
 */
public class RoundRobinWeight {

	private static int cw = 0;
	private static int number = -1;// 当前SERVER的序号,开始是-1
	private static int max;// 最大权重
	private static int gcd;// 最大公约数
	private static Map<String,ServerInfo[]> MasterSlaveList = new HashMap<String, ServerInfo[]>(); //从服务器list
	
	@Autowired
	private RedisBast t;
	
	@Value("#{configProperties['common.redis.server.ip']}")
    private String serverIp;
	
	@Value("#{configProperties['common.redis.server.port']}")
	private String serverPort;
	
	@Value("#{configProperties['common.redis.server.weight']}")
	private String serverWeight;
	
	
	public void setServerWeight(String serverWeight) {
		this.serverWeight = serverWeight;
	}


	public void setServerPort(String serverPort) {
		this.serverPort = serverPort;
	}


	public void setServerIp(String serverIp) {
		this.serverIp = serverIp;
	}

	
	
	
	
	/**
	 * 
	 * 功能说明：初始化所有的主从服务器
	 * 日期:	2015年5月4日
	 * 开发者:宋艳垒
	 *
	 */
	@PostConstruct
	public void initAllMasterSlave (){
		//ip集合
		String[] allIps = this.serverIp.split(":");
		//端口集合
		String[] allPorts = this.serverPort.split(":");
		//权重集合
		String[] allWeights = this.serverWeight.split(":");
		for (int i = 0; i < allIps.length; i++) {
			String[] ipArray = allIps[i].split(",");
			String[] portArray = allPorts[i].split(",");
			String[] weightArray = allWeights[i].split(",");
			ServerInfo[] server = new ServerInfo[ipArray.length];// 机器序号：权重
			for (int j = 0; j < ipArray.length; j++) {
				ServerInfo si = new ServerInfo();
				si.setWeight(Integer.parseInt(weightArray[j]));
				si.setIp(ipArray[j]);
				si.setPort(Integer.parseInt(portArray[j]));
				server[j] = si;
			}
			MasterSlaveList.put(ipArray[0]+portArray[0],server);
		}
	}
	
	/**
	 * 
	 * 功能说明：获取redis数据源
	 * 日期:	2015年5月4日
	 * 开发者:宋艳垒
	 *
	 */
	public Jedis getRedisSource(String key){
		Jedis jedis = t.select(key);
		ServerInfo[] resServer = MasterSlaveList.get(jedis.getClient().getHost()+jedis.getClient().getPort());
		//找出最大权重的主机
		if(!StringUtils.isEmpty(resServer)){
			max = getMaxWeight(resServer);
			//初始化最大公约数
			gcd = gcd(resServer);
			ServerInfo si = RoundRobinWeight.next(resServer);
			return new Jedis(si.getIp(),si.getPort());
		}
		return null;
		
	}
	
	

	/**
	 * 求最大公约数
	 * 
	 * @param array
	 * @return
	 */
	public static int gcd(ServerInfo[] ary) {

		int min = ary[0].getWeight();
		for (int i = 0; i < ary.length; i++) {
			if(!StringUtils.isEmpty(ary[i])){
				if (ary[i].getWeight() < min) {
					min = ary[i].getWeight();
				}
			}
		}
		while (min >= 1) {
			boolean isCommon = true;
			for (int i = 0; i < ary.length; i++) {
				if(!StringUtils.isEmpty(ary[i])){
				if (ary[i].getWeight() % min != 0) {
					isCommon = false;
					break;
				}
				}
			}
			if (isCommon) {
				break;
			}
			min--;
		}
		return min;
	}

	/**
	 * 求最大值，权重
	 * 
	 * @return
	 */

	public static int getMaxWeight(ServerInfo[] ary) {
		int max = 0;
		for (int i = 0; i < ary.length; i++) {
			if(!StringUtils.isEmpty(ary[i])){
				if (max < ary[i].getWeight()) {
					max = ary[i].getWeight();
				}
			}
		}
		return max;
	}

	/**
	 * 
	 * 获取请求主机信息
	 * @return
	 */
	public static ServerInfo next(ServerInfo[] ary) {
		if(!StringUtils.isEmpty(ary)){
			while (true) {
				number = (number + 1) % ary.length;
				if (number == 0) {
					cw = cw - gcd;// cw比较因子，从最大权重开始，以最大公约数为步长递减
					if (cw <= 0) {//
						cw = max;
						if (cw == 0)
							return null;
					}
				}
				if(!StringUtils.isEmpty(ary[number])){
					if (ary[number].getWeight() >= cw)
						return ary[number];
				}
			}
		}
		return null;

	}
}
