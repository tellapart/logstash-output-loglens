/**
 * Copyright Twitter 2016
 */
package com.twitter.loglens;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransportException;
import scribe.thrift.scribe.Client;
import scribe.thrift.LogEntry;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

/**
 * A simple class to that send messages to a scribe endpoint using thrift over https
 */
public class LoglensConnector {
  private static final int MAX_BUFFER_SIZE = 5000;

  private String url;
  private String authorization;
  private String category;
  private Object bufferLock = new Object();

  private volatile ArrayList<LogEntry> buffer;
  private final ExecutorService exec = Executors.newFixedThreadPool(1);

  private Client scribeClient;
  private THttpClient transport;

  public LoglensConnector(String url, String bearerToken, String scribeCategory)
  {
    this.url = url;
    this.authorization = "Bearer " + bearerToken;
    this.category = scribeCategory;
    synchronized (bufferLock) {
      buffer = new ArrayList<LogEntry>();
    }
    try {
      open();
    } catch (TTransportException e)
    {
      System.err.println(e);
    }
  }

  private void open() throws TTransportException{
    try {
      transport = new THttpClient(this.url);
      transport.setCustomHeader("X-B3-Flags", "1");
      transport.setCustomHeader("Authorization", authorization);
      TBinaryProtocol protocol = new TBinaryProtocol(transport);
      transport.open();
      scribeClient = new Client(protocol);
    } catch (TTransportException e) {
      System.err.println(ZonedDateTime.now() + "Failed to open new connection");
      throw e;
    }
  }

  public void close(){
    if (transport.isOpen()) {
      try {
        transport.flush();
      } catch (Exception e) {
        System.err.println(e);
      }
      transport.close();
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
      buffer.add(new LogEntry(this.category, message));
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
        if (!transport.isOpen())
        {
          System.err.println("Opening new connection.");
          open();
        }
        scribeClient.Log(toSend);
      } catch (Exception e) {
        System.err.println(e);
        System.err.println(ZonedDateTime.now() + " Buffered Messages: " + toSend.size() );
        if (toSend.size() > MAX_BUFFER_SIZE) {
          System.err.println("Purged Buffer");
        } else if (!toSend.isEmpty()) {
          synchronized (bufferLock) {
            buffer.addAll(toSend);  // add messages in toSend back to the buffer
                                    // since they were not sent
          }
        }
      }
    }
  };
}


