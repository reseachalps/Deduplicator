package it.unimore.cleaning;

import java.util.ArrayList;

public class UrlCleaner extends StringCleaner{

	@Override
	public ArrayList<String> compareWith() {
		ArrayList<String> l = new ArrayList<String>();
		
		l.add(" ");
		l.add("https://");
		l.add("http://");
		l.add("/");
		
		return l;
	}

}
