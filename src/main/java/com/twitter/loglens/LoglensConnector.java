/**
 * Copyright Twitter 2016
 */
package com.twitter.loglens;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.THttpClient;
import scribe.thrift.scribe.Client;
import scribe.thrift.LogEntry;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

/**
 * A simple class to that send messages to a scribe endpoint using thrift over https
 */
public class LoglensConnector {

  private String url;
  private String authorization;
  private Object bufferLock = new Object();

  private volatile ArrayList<LogEntry> buffer;
  private final ExecutorService exec = Executors.newFixedThreadPool(1);

  public LoglensConnector(String url, String bearerToken)
  {
    this.url = url;
    this.authorization = "Bearer " + bearerToken;
    synchronized (bufferLock) {
      buffer = new ArrayList<LogEntry>();
    }
  }

  /**
   * Print message to standard output
   * @param message
   */
  public void printMessage(String message)
  {
    System.out.println(message);
  }

  /**
   * Puts message in a buffer and sends is async
   * @param message
   */
  public void sendMessage(String message) {
    synchronized (bufferLock) {
      buffer.add(new LogEntry("muxer_monitor_source", message));
    }
    exec.execute(sendMessageAsync);
  }

  /**
   * Sends messages in the buffer
   */
  private Runnable sendMessageAsync = new Runnable() {
    public void run() {
      ArrayList<LogEntry> toSend;
      synchronized (bufferLock)
      {
        if (buffer.isEmpty()){
          return;
        } else {
          toSend = buffer;
          buffer = new ArrayList<LogEntry>();
        }
      }
      try {
        THttpClient transport = new THttpClient(url);
        transport.setCustomHeader("X-B3-Flags", "1");
        transport.setCustomHeader("Authorization", authorization);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        transport.open();
        Client scribeClient = new Client(protocol);
        scribeClient.Log(toSend);
        toSend.clear();
        transport.close();
      } catch (TException e) {
        if (!toSend.isEmpty()) {
          synchronized (bufferLock) {
            buffer.addAll(toSend);  // add messages in toSend back to the buffer
                                    // since they were probably not sent
          }
        }
        System.err.println(e);
      }
    }
  };
}


