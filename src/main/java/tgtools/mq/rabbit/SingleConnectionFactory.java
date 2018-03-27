package tgtools.mq.rabbit;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import tgtools.exceptions.APPErrorException;
import tgtools.util.StringUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 田径
 * @Title
 * @Description
 * @date 9:46
 */
public class SingleConnectionFactory {

    private static Map<String, ConnectionFactory> mFactorys = new HashMap<String, ConnectionFactory>();
    private static Map<String, Connection> mConnections = new HashMap<String, Connection>();


    public synchronized static Connection get(String pName) throws APPErrorException {
        if(StringUtil.isNullOrEmpty(pName))
        {
            throw new APPErrorException("连接名称不能为空");
        }
        if (!mConnections.containsKey(pName)) {
            if (!mFactorys.containsKey(pName)) {
                throw new APPErrorException("不存在ConnectionFactory，Name:" + pName);
            }

            try {
                Connection conn= mFactorys.get(pName).newConnection();
                mConnections.put(pName,conn);
            } catch (Exception e) {
               throw new APPErrorException("创建连接失败，Name:"+pName+"，原因："+e.getMessage(),e);
            }
        }
        return mConnections.get(pName);
    }
    public static void clear()
    {
        for(Map.Entry<String, Connection> item:mConnections.entrySet())
        {
            try {
                item.getValue().close();
            } catch (Exception e) {
            }
        }
        mConnections.clear();
        mFactorys.clear();
    }

    public static void add(String pName, ConnectionFactory pFactory) {
        mFactorys.put(pName, pFactory);
    }

    public static void add(String pName, String pHost, int pPort, String pUserName, String pPassword,
                           boolean pAutomaticRecoveryEnabled) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(pHost);
        factory.setUsername(pUserName);
        factory.setPassword(pPassword);
        factory.setPort(pPort);
        factory.setAutomaticRecoveryEnabled(pAutomaticRecoveryEnabled);
        add(pName, factory);
    }
}
