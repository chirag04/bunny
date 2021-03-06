package org.rabix.engine.processor;

import java.util.UUID;

import org.rabix.engine.event.Event;
import org.rabix.engine.processor.handler.EventHandlerException;

public interface EventProcessor {

  void start();

  void stop();

  boolean isRunning();

  void send(Event event) throws EventHandlerException;

  void addToQueue(Event event);
  
  void addToExternalQueue(Event event);
  
  void persist(Event event);

  public static class EventProcessorDispatcher {

    public static int dispatch(UUID rootId, int numberOfEventProcessors) {
      return Math.abs(rootId.hashCode() % numberOfEventProcessors);
    }

  }

}
