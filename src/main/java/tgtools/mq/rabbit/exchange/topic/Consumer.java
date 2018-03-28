package tgtools.mq.rabbit.exchange.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import tgtools.exceptions.APPErrorException;

import java.util.List;

/**
 * @author 田径
 * @Title
 * @Description
 * @date 10:58
 */
public class Consumer extends tgtools.mq.rabbit.exchange.direct.Consumer {
    private List<String> mRoutingKeys;

    public List<String> getRoutingKeys() {
        return mRoutingKeys;
    }

    public void setRoutingKeys(List<String> pRoutingKeys) {
        mRoutingKeys = pRoutingKeys;
    }

    @Override
    protected void queueDeclare() throws APPErrorException {
        try {
            getChannel().exchangeDeclare(getExchangeName(),  BuiltinExchangeType.TOPIC);
            getChannel().queueDeclare(getQueueName(), true, false, false, null);
            if(null!=getRoutingKeys()&&getRoutingKeys().size()>0){
                for(int i=0;i<getRoutingKeys().size();i++){
                    getChannel().queueBind(getQueueName(), getExchangeName(), getRoutingKeys().get(i));
                }
            }else {
                getChannel().queueBind(getQueueName(), getExchangeName(), getRoutingKey());
            }
        }catch (Exception e)
        {
            throw new APPErrorException("exchange："+getExchangeName()+"绑定 queue:"+getQueueName()+"失败；原因："+e.getMessage(),e);
        }
    }
}
