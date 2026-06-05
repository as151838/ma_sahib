package de.rwthaachen.dbis.i5cloudmatch.controller;


public class GlobalIDGenerator {
	private static int globalID;
	public static int getGlobalID() {
		return ++globalID;
	}

}
