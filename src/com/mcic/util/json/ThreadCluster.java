package com.mcic.util.json;

import java.util.Stack;

public class ThreadCluster {
	private Stack<Thread> active;
	private Stack<Thread> queued;
	private Thread dispatcher;
	private boolean dispatchingCompleted;
	private int concurrency;
	
	public ThreadCluster(int c) {
		active = new Stack<Thread>();
		queued = new Stack<Thread>();
		dispatcher = null;
		concurrency = c;
		dispatchingCompleted = false;
	}
	
	public void dispatch(Runnable r) {
		Thread t = new Thread(r);
		queued.push(t);
		if (dispatcher == null || !dispatcher.isAlive()) {
			dispatcher = new Thread(new Runnable() {
				public void run() {
					while (!dispatchingCompleted || queued.size() > 0 || active.size() > 0) {
						clean();
						while (active.size() < concurrency && queued.size() > 0) {
							Thread t = queued.pop();
							t.start();
							active.push(t);
						}
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			});
			dispatcher.start();
		}
	}
	
	public void clean() {
		Stack<Thread> newThreads = new Stack<Thread>();
		while (active.size() > 0) {
			Thread t = active.pop();
			if (t.isAlive()) {
				newThreads.push(t);
			}
		}
		active = newThreads;
	}
	
	public void join() {
		dispatchingCompleted = true;
		if (dispatcher != null) {
			try {
				dispatcher.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
