package tgtools.mq.rabbit.exchange.fanout;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Connection;
import tgtools.exceptions.APPErrorException;
import tgtools.mq.rabbit.AbstractProducer;
import tgtools.util.StringUtil;

import java.io.IOException;
import java.util.Map;

/**
 * @author 田径
 * @Title
 * @Description
 * @date 9:55
 */
public class Producer extends AbstractProducer {

    public Producer() {
    }


    private String mExchangeName;

    public String getExchangeName() {
        return mExchangeName;
    }

    public void setExchangeName(String pExchangeName) {
        mExchangeName = pExchangeName;
    }


    public void init(Connection pConnection, String pExchangeName) throws APPErrorException {
        init(pConnection, pExchangeName, true, false, false, null);
    }

    public void init(Connection pConnection, String pExchangeName, boolean pDurable) throws APPErrorException {
        init(pConnection, pExchangeName, pDurable, false, false, null);
    }

    public void init(Connection pConnection, String pExchangeName, boolean pDurable, boolean pExclusive, boolean pAutoDelete,
                     Map<String, Object> pArguments) throws APPErrorException {
        try {
            setChannel(pConnection.createChannel());
            setExchangeName(pExchangeName);
            queueDeclare();
        } catch (Exception e) {
            throw new APPErrorException("创建出错！原因：" + e.getMessage(), e);
        }

    }
    @Override
    public void init(Connection pConnection) throws APPErrorException {
        init(pConnection,getExchangeName());
    }
    protected void queueDeclare() throws APPErrorException {
        try {
            getChannel().exchangeDeclare(mExchangeName, BuiltinExchangeType.FANOUT);
        } catch (IOException e) {
            throw new APPErrorException("定义队列出错！");
        }
    }

    @Override
    protected void queueDeclare(boolean pDurable, boolean pExclusive, boolean pAutoDelete,
                                Map<String, Object> pArguments) throws APPErrorException {
        try {
            getChannel().exchangeDeclare(mExchangeName, BuiltinExchangeType.FANOUT, pDurable, pExclusive, pAutoDelete, pArguments);
        } catch (IOException e) {
            throw new APPErrorException("定义队列出错！");
        }
    }

    @Override
    public void send(byte[] pMessage, AMQP.BasicProperties pBasicProperties) throws APPErrorException {
        try {
            getChannel().basicPublish(mExchangeName, StringUtil.EMPTY_STRING, pBasicProperties, pMessage);
        } catch (IOException e) {
            throw new APPErrorException("发送队列失败", e);
        }
    }




}
