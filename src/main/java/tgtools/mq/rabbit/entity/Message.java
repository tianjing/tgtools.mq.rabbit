package tgtools.mq.rabbit.entity;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import tgtools.exceptions.APPErrorException;

import java.io.UnsupportedEncodingException;

/**
 * @author 田径
 * @Title
 * @Description
 * @date 11:20
 */
public class Message {
    public Message(Object pSender, String pConsumerTag, Envelope pEnvelope, AMQP.BasicProperties pProperties, byte[] pBody){
        this(pSender,pConsumerTag,pEnvelope,pProperties,pBody,"UTF-8");
    }
    public Message(Object pSender, String pConsumerTag, Envelope pEnvelope, AMQP.BasicProperties pProperties, byte[] pBody, String pCharsetName){
        mConsumerTag=pConsumerTag;
        mEnvelope=pEnvelope;
        mProperties=pProperties;
        mBody=pBody;
        mSender=pSender;
        mCharsetName=pCharsetName;
    }
    private String mCharsetName;
    private String mConsumerTag;
    private Envelope mEnvelope;
    private AMQP.BasicProperties mProperties;
    private Object mSender;
    private byte[] mBody;

    public String getConsumerTag() {
        return mConsumerTag;
    }

    public Envelope getEnvelope() {
        return mEnvelope;
    }

    public AMQP.BasicProperties getProperties() {
        return mProperties;
    }

    public Object getSender() {
        return mSender;
    }

    public byte[] getBody() {
        return mBody;
    }

    public String getBodyString() throws APPErrorException {
        try {
            return new String(mBody,mCharsetName);
        } catch (UnsupportedEncodingException e) {
            throw new APPErrorException("字符串转换出错",e);
        }
    }
}
