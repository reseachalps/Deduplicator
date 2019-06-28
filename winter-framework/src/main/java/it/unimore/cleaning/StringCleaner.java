package it.unimore.cleaning;

import java.util.ArrayList;

public abstract class StringCleaner {
	
	//cleaning algorithm
	public String clean(String s) {
		if(s != null) {
			//test
			//System.out.println("String before cleaning: "+ s);
			ArrayList<String> l = compareWith();
			
			for (String cmp : l) {
				//CompareWith() return only lowerCase strings
				String ls = s.toLowerCase();
				
				while(ls.contains(cmp)) {
					int i = ls.indexOf(cmp);
					
					//Using the index to modify the original string
					s = s.substring(0, i) + s.substring(i+cmp.length());
					
					//Updating the lowerCase string
					ls = s.toLowerCase();
					
					//test
				//System.out.println(s);
				}
			
			}
			//test
			//System.out.println("String after cleaning:"+ s);
		}
		
		return s;
	}

	//terms to clean
	protected abstract ArrayList<String> compareWith();
}
