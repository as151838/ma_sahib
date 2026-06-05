package de.rwthaachen.dbis.i5cloudmatch.model;

import java.util.ArrayList;
import java.util.List;

import de.rwthaachen.dbis.i5cloudmatch.model.Attribute;

public class NeighborRBC {
		public  String globalDataSource;
		public  int globalID;
		public  String entityType;
		public  String clusterLabel;
		public  Attribute attribute;
		public  List<Attribute> attributes= new ArrayList<Attribute>();
}
