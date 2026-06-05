package de.rwthaachen.dbis.i5cloudmatch.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.hsqldb.lib.Iterator;

import com.google.gson.Gson;

//import com.google.gson.Gson;

public class Entity {
	public String globalDataSource;
	public int globalID;
	public String entityType;
	public Map<Entity,String> relationalNeighborEntites=new HashMap<Entity,String>();
	public Map<Entity,String> blockNeighborEntites=new HashMap<Entity,String>();
	
	public void print(){
		System.out.print("globalID:"+globalID+" ");
		System.out.print("relationalNeighborEntites:");
		String eType1; Entity eObject1;
		Object[] eRefArray=relationalNeighborEntites.entrySet().toArray();
		for (int i=0;i<eRefArray.length;i++) {
			eType1=((Entry<Object, String>) eRefArray[i]).getValue();
			eObject1=(Entity)((Entry<Object, String>) eRefArray[i]).getKey();
			System.out.print(eObject1.globalID+"|");
		}
		System.out.println();
	}
	public String serialize(){
		Gson gson = new Gson();
		String json = gson.toJson(this); 
		return json;
	}
	public Entity deserialize (String string) {
		Gson gson = new Gson();
		Entity entity= gson.fromJson(string, Entity.class);
		return entity;
	}

	public int getGlobalID() {
		return this.globalID;
	}
}

