package com.mcic.util.json;

public class Pair<T, U> implements Comparable<T> {
	public T first;
	public U second;
	
	public Pair(T first, U second){
		super();
		this.first = first;
		this.second = second;
	}
	
	@Override
	public String toString() {
		return "[" + first + ", " + second + "]";
	}

	@Override
	public boolean equals(Object obj) {
		Pair<T, U> o = (Pair<T, U>)obj;
		return first.equals(o.first) && second.equals(o.second);
	}

	@Override
	public int compareTo(Object o) {
		Pair<T, U> p = (Pair<T, U>)o;
		int i = ((Comparable<T>) first).compareTo(p.first);
		if (i == 0) {
			i = ((Comparable<U>) second).compareTo(p.second);
		}
		return i;
	}
	
	
}
