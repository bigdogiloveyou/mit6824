package labs.lab1.mapreduce2.other;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.AbstractUserProcessor;

/**
 * @author xushu
 * @create 8/9/21 11:12 PM
 * @desc
 */
public abstract class MessageProcessor<T> extends AbstractUserProcessor<T> {


    @Override
    public void handleRequest(BizContext bizCtx, AsyncContext asyncCtx, T request) {
        return;
    }

    @Override
    public String interest() {
        return Request.class.getName();
    }
}
