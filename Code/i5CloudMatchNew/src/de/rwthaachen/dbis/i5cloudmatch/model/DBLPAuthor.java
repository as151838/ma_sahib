package de.rwthaachen.dbis.i5cloudmatch.model;

import java.io.Serializable;
import java.util.HashMap;

public class DBLPAuthor extends Entity implements Serializable{
	public String name;
	public DBLPAuthor (String name) {
		this.name=name;
	}
	
	public String getEntityInfo(){
		String entityInfo;
		entityInfo=
				"<entityInfo> "+
				"<globalDataSource "+this.globalDataSource+"> "+		
				"<globalID "+this.globalID+"> "+
				"<entityType "+this.entityType+"> "+
				"<name "+this.name+"> "+
				"</entityInfo> ";
		return entityInfo;
	}

}
