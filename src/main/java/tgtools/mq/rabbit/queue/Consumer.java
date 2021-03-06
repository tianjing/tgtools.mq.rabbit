package tgtools.mq.rabbit.queue;

import com.rabbitmq.client.Connection;
import tgtools.exceptions.APPErrorException;
import tgtools.mq.rabbit.AbstractConsumer;
import tgtools.mq.rabbit.listen.IMessageListener;

import java.util.Map;

/**
 * @author 田径
 * @Title
 * @Description
 * @date 10:58
 */
public class Consumer extends AbstractConsumer {
    public Consumer() {
    }

    private Map<String, Object> mDeclareArguments=null;

    public Map<String, Object> getDeclareArguments() {
        return mDeclareArguments;
    }

    public void setDeclareArguments(Map<String, Object> pDeclareArguments) {
        mDeclareArguments = pDeclareArguments;
    }

    @Override
    public void init(Connection pConnection) throws APPErrorException {
        init(pConnection,getQueueName(),getMessageListening());
    }
    public void init(Connection pConnection, String pQueueName,Map<String, Object> pDeclareArguments, IMessageListener pMessage) throws APPErrorException {
        try {
            setChannel(pConnection.createChannel());
            setQueueName(pQueueName);
            setMessageListening(pMessage);
            setDeclareArguments(pDeclareArguments);
            queueDeclare();
            createDefaultConsumer(getChannel());
        } catch (Exception e) {
            throw new APPErrorException("初始化Channel出错；原因：" + e.getMessage(), e);
        }
    }
    public void init(Connection pConnection, String pQueueName, IMessageListener pMessage) throws APPErrorException {
        try {
            setChannel(pConnection.createChannel());
            setQueueName(pQueueName);
            setMessageListening(pMessage);
            queueDeclare();
            createDefaultConsumer(getChannel());
        } catch (Exception e) {
            throw new APPErrorException("初始化Channel出错；原因：" + e.getMessage(), e);
        }
    }

    protected void queueDeclare() throws APPErrorException {
        try {
            getChannel().queueDeclare(getQueueName(), true, false, false, mDeclareArguments);
        }catch (Exception e)
        {
            throw new APPErrorException("绑定 queue:"+getQueueName()+"失败；原因："+e.getMessage(),e);
        }
    }

}
