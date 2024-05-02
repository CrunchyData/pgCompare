package com.crunchydata.services;

import com.crunchydata.model.DataCompare;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class msgQueue {
    BlockingQueue<DataCompare[]> q;
    int capacity;

    msgQueue(int cap) {
        BlockingQueue<DataCompare[]> q = new ArrayBlockingQueue<>(cap);
        capacity=cap;
    }

    public synchronized void publish(DataCompare[] dc) throws InterruptedException {
        String name=Thread.currentThread().getName();
        while(q.size() == capacity){
            System.out.println("Queue Full!"+name+" waiting for message to be consumed...");
            wait();
        }
        q.add(dc);
        notifyAll();
    }

    public synchronized void consume() throws InterruptedException {
        String name=Thread.currentThread().getName();
        while(q.size()==0){
            System.out.println(name+" waiting for new message...");
            wait();
        }
        DataCompare[] dc = q.poll();
        notifyAll();
    }
}
