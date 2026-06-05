package de.rwthaachen.dbis.i5cloudmatch.model;

import java.util.HashMap;

public class DBLPProceeding extends Entity{
	public String booktitle;
	public DBLPProceeding (String booktitle) {
		this.booktitle=booktitle;
	}
	
	public String getEntityInfo(){
		String entityInfo;
		entityInfo=
				"<entityInfo> "+
				"<globalDataSource "+this.globalDataSource+"> "+		
				"<globalID "+this.globalID+"> "+
				"<entityType "+this.entityType+"> "+
				"<booktitle "+this.booktitle+"> "+
				"</entityInfo> ";
		return entityInfo;
	}


}
