package de.rwthaachen.dbis.i5cloudmatch.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import de.rwthaachen.dbis.i5cloudmatch.model.DBLPAuthor;

public class RelationalBlockedEntityCluster {
	public String globalDataSource;
	public int globalID;
	public String entityType;
	public String clusterLabel;
	public List<AttributeOld> attributes= new ArrayList<AttributeOld>();
	public RelationalBlockedEntityCluster anotherEntity;
	public DBLPAuthor dblpauthor= new DBLPAuthor("Ammar");
	public Map<RelationalBlockedEntityCluster,String> relationalNeighborEntityClusters=
			new HashMap<RelationalBlockedEntityCluster,String>();
	public Map<RelationalBlockedEntityCluster,String> blockNeighborEntityClusters=
			new HashMap<RelationalBlockedEntityCluster,String>();
	public Map<RelationalBlockedEntityCluster,String> mergedEntityClusters=
			new HashMap<RelationalBlockedEntityCluster,String>();

	public void printInfo() {
		System.out.println("globalDataSource="+globalDataSource);
		System.out.println("globalID="+globalID);
		System.out.println("type="+entityType);
		System.out.println("clusterLabel="+clusterLabel);
		for (int i=0; i< attributes.size();i++) 
			System.out.println("attribute"+i+"="+attributes.get(i).type+attributes.get(i).name+attributes.get(i).value);
	}
	public String serialize(){
		Gson gson = new Gson();
		String json = gson.toJson(this); 
		return json;
	}
	
	public RelationalBlockedEntityCluster deserialize (String string) {
		Gson gson = new Gson();
		JsonParser parser= new JsonParser();
		//RelationalBlockedEntityCluster rbc=new RelationalBlockedEntityCluster();
	    //rbc= gson.fromJson(string, RelationalBlockedEntityCluster.class);
		JsonObject obj= parser.parse(string).getAsJsonObject();
		RelationalBlockedEntityCluster rbc=gson.fromJson(obj,RelationalBlockedEntityCluster.class);
		return rbc;
	}
	//canonication operations
	public static void main(String[] args) {
		RelationalBlockedEntityCluster a,b,n;
		a=new RelationalBlockedEntityCluster();
		n=new RelationalBlockedEntityCluster();
		a.globalDataSource="DBLP";
		a.globalID=1;
		a.entityType="author";
		a.anotherEntity= new RelationalBlockedEntityCluster();
		a.anotherEntity.globalDataSource="DBLP";
		a.attributes.add(new AttributeOld("String","name","Sahib Ammar"));
		a.relationalNeighborEntityClusters.put(n, "author");

		
		String str=a.serialize();
		System.out.println(str);
		b=new RelationalBlockedEntityCluster();
		//b=b.deserialize(str);
		//System.out.println(b.anotherEntity.globalDataSource);
		//System.out.println(b.globalDataSource);
	}
}	


