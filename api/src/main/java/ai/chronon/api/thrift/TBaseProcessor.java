package ai.chronon.api.thrift;

import java.util.Collections;
import java.util.Map;

import ai.chronon.api.thrift.*;
import ai.chronon.api.thrift.TBase;
import ai.chronon.api.thrift.TException;
import ai.chronon.api.thrift.TProcessor;
import ai.chronon.api.thrift.protocol.TMessage;
import ai.chronon.api.thrift.protocol.TMessageType;
import ai.chronon.api.thrift.protocol.TProtocol;
import ai.chronon.api.thrift.protocol.TProtocolUtil;
import ai.chronon.api.thrift.protocol.TType;

public abstract class TBaseProcessor<I> implements TProcessor {
  private final I iface;
  private final Map<String, ProcessFunction<I, ? extends ai.chronon.api.thrift.TBase, ? extends ai.chronon.api.thrift.TBase>> processMap;

  protected TBaseProcessor(
      I iface,
      Map<String, ProcessFunction<I, ? extends ai.chronon.api.thrift.TBase, ? extends ai.chronon.api.thrift.TBase>> processFunctionMap) {
    this.iface = iface;
    this.processMap = processFunctionMap;
  }

  public Map<String, ProcessFunction<I, ? extends TBase, ? extends TBase>> getProcessMapView() {
    return Collections.unmodifiableMap(processMap);
  }

  @Override
  public void process(TProtocol in, TProtocol out) throws TException {
    TMessage msg = in.readMessageBegin();
    ProcessFunction fn = processMap.get(msg.name);
    if (fn == null) {
      TProtocolUtil.skip(in, TType.STRUCT);
      in.readMessageEnd();
      TApplicationException x =
          new TApplicationException(
              TApplicationException.UNKNOWN_METHOD, "Invalid method name: '" + msg.name + "'");
      out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
      x.write(out);
      out.writeMessageEnd();
      out.getTransport().flush();
    } else {
      fn.process(msg.seqid, in, out, iface);
    }
  }
}
