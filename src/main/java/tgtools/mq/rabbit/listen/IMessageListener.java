package tgtools.mq.rabbit.listen;

import tgtools.mq.rabbit.entity.Message;

/**
 * @author 田径
 * @Title
 * @Description
 * @date 11:19
 */
public interface IMessageListener {
    void onMessage(Message pMsg);
}
