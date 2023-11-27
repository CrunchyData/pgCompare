package com.crunchydata.util;

public class ThreadSync {

    public boolean sourceComplete = false;
    public boolean targetComplete = false;

    public synchronized void ObserverWait() {
            try {
                wait();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
    }

    public synchronized void ObserverNotify() {
        try {
            notifyAll();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

}
