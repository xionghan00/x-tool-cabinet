package io.xh.xtoolcabinet.component.queue;

public interface MsgListener {

	ConsumeResultAction consume(ConsumeContext context);
}
