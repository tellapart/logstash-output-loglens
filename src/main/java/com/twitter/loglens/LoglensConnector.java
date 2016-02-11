/**
 * Copyright Twitter 2016
 */
package com.twitter.loglens;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransportException;
import scribe.thrift.scribe.Client;
import scribe.thrift.LogEntry;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

/**
 * A simple class to that send messages to a scribe endpoint using thrift over https
 */
public class LoglensConnector {

  private static final int MAX_NO_MESSAGES_PER_CALL = 20;
  private static final int MAX_BUFFER_SIZE = 1000000;

  private String url;
  private String authorization;
  private String category;

  private final ExecutorService exec = Executors.newFixedThreadPool(1);
  private final ConcurrentLinkedQueue<LogEntry> queue = new ConcurrentLinkedQueue<LogEntry>();

  private Client scribeClient;
  private THttpClient transport;

  public LoglensConnector(String url, String bearerToken, String scribeCategory)
  {
    this.url = url;
    this.authorization = "Bearer " + bearerToken;
    this.category = scribeCategory;
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
   * Puts message in a queue and send them async
   * @param message
   */
  public void sendMessage(String message) {
    queue.offer(new LogEntry(this.category, message));
    exec.execute(sendMessageAsync);
  }

  /**
   * Sends messages in the queue
   */
  private Runnable sendMessageAsync = new Runnable() {
    public void run() {
      LogEntry log = queue.poll();
      while (log != null) {
        int queueSize = queue.size();
        if (queueSize > MAX_BUFFER_SIZE)
        {
          queue.clear();
          System.err.println(ZonedDateTime.now() + " Log buffer too big, purged! " + queueSize);
          return;
        }

        ArrayList<LogEntry> toSend = new ArrayList<LogEntry>();
        int n = 0;

        while (log != null && n < MAX_NO_MESSAGES_PER_CALL) {
          // scribeClient.log takes a list, however the twitter api rejects
          // requests that are too long, so we only send MAX_NO_MESSAGES_PER_CALL
          // logs at a time...
          toSend.add(log);
          log = queue.poll();
          n++;
        }

        try {
          if (!transport.isOpen()) {
            System.err.println("Opening new connection.");
            open();
          }
          scribeClient.Log(toSend);
        } catch (Exception e) {
          System.err.println(e);
          System.err.println(ZonedDateTime.now() + " Failed to send " + toSend.size() + " messages, buffer size: " + queue.size());
          queue.addAll(toSend);  // add messages in toSend back to the queue
          if (log == null) {
            log = queue.poll();
          }
        }
      }
    }
  };
}


