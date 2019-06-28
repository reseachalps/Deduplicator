package it.unimore.cleaning;

import java.util.ArrayList;

public class LabelCleaner extends StringCleaner {

    @Override
    protected ArrayList<String> compareWith() {
        ArrayList<String> l = new ArrayList<String>();

        l.add(" .");
        l.add("s.r.l.");
        l.add("s.r.l");
        l.add("s.n.c");
        l.add("s.n.c.");
        l.add(" srl");
        l.add("s.p.a.");
        l.add("s.p.a");
        l.add("spa");
        l.add("societa responsabilita limitata");
        l.add(" di ");
        l.add(" delle ");
        l.add(" of ");
        return l;
    }

}
