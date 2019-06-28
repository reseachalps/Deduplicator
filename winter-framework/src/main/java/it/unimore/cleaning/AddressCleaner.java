package it.unimore.cleaning;

import java.util.ArrayList;

public class AddressCleaner extends StringCleaner{

	@Override
	protected ArrayList<String> compareWith() {
		ArrayList<String> l = new ArrayList<String>();
		
		l.add(",");
		l.add(".");
		l.add("viale ");
		l.add("vle ");
		l.add("via ");
		l.add("piazzale ");
		l.add("piazza ");
		l.add("pza ");
		l.add("corso ");
		l.add("cso ");
		l.add("strada ");
		l.add("sda ");
		l.add("lgo ");
		
		return l;
		
	}

}
