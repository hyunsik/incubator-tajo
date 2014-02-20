package org.apache.tajo.master.rm;

import org.apache.hadoop.yarn.event.AbstractEvent;

public class NodeEvent extends AbstractEvent<NodeEventType> {
  public NodeEvent(NodeEventType nodeEventType) {
    super(nodeEventType);
  }
}
