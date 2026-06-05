package de.rwthaachen.dbis.i5cloudmatch.model;

public class Token {
	public String value;
	public void clean(){
		value=value.replaceAll("\\.", "");
		value=value.replaceAll("\'", "");
		value=value.replaceAll("-", "");
		value=value.replaceAll(":", "");
		value=value.replaceAll("\\(", "");
		value=value.replaceAll("\\)", "");
	}
	public void toLower() {
		value=value.toLowerCase();
	}
	public void discardCommonWords() {
		
	}
	public Token(String value) {
		this.value=value;
		this.clean();
		this.toLower();
		
	}
	public static void main(String args[]){
		Token token1= new Token("A. H' Abdul-Hameed");
		System.out.println(token1.value);
		Token token2= new Token("(KRW'10)");
		System.out.println(token2.value);
		Token token3= new Token("A comparison of string distance metrics for name-matching tasks");
		System.out.println(token3.value);
		Token token4= new Token("Swoosh: A generic approach to entity resolution");
		System.out.println(token4.value);
		Token token5= new Token("Multi-relational record linkage");
		System.out.println(token5.value);
		String str="Ammar Sahib";
		str=str.replaceAll("\\s", "%%%");
		System.out.println(str);
		str=str.replaceAll("%%%"," ");
		System.out.println(str);
	}
	
}
