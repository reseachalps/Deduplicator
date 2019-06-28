package it.unimore.deduplication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import it.unimore.cleaning.AddressCleaner;
import it.unimore.cleaning.DBCleaner;
import it.unimore.cleaning.LabelCleaner;
import it.unimore.cleaning.UrlCleaner;
import it.unimore.deduplication.model.Organization;
import org.apache.jena.base.Sys;

public class OrganizationDBCleaner implements DBCleaner {

    @Override
    public void cleanDB(DataSet<Record, Attribute> dataset) {
        DataSet<Record, Attribute> ds = (HashedDataSet) dataset;
        Collection<Record> c = ds.get();

        //Setting up cleaners
        UrlCleaner u = new UrlCleaner();
        AddressCleaner a = new AddressCleaner();
//        LabelCleaner l = new LabelCleaner();

        List<String> replace = new ArrayList<>();
        List<String> substitution = new ArrayList<>();


        replace.add("universita");
        replace.add("università");
        replace.add("université");
        substitution.add("university");
        substitution.add("university");
        substitution.add("university");

        replace.add("universität");
        substitution.add("university");


        replace.add("universitá");
        substitution.add("university");

        //cities
        replace.add("roma");
        substitution.add("rome");


        replace.add("napoli");
        substitution.add("naples");
        replace.add("milano");
        substitution.add("milan");

        replace.add("roma");
        substitution.add("rome");

        replace.add("torino");
        substitution.add("turin");

        replace.add("firenze");
        substitution.add("florence");


        List<String> stopwords = new ArrayList<>();
        List<String> stopwordsSpace = new ArrayList<>();
        stopwordsSpace.add("dipartimento");
        stopwordsSpace.add("department");
        stopwordsSpace.add(" facoltà ");
        stopwordsSpace.add(" facolta ");
        stopwordsSpace.add("deptof ");
        stopwordsSpace.add(" studi ");
        stopwordsSpace.add(" degli ");
        stopwords.add("institute");


        stopwordsSpace.add(" and ");
        stopwordsSpace.add(" e ");
        stopwordsSpace.add(" of ");
        stopwordsSpace.add(" di ");
        stopwordsSpace.add(" de ");
        stopwordsSpace.add(" fo ");
        stopwordsSpace.add("'");
        stopwordsSpace.add("’");
        stopwordsSpace.add("‘");

        stopwords.add("startup costituita a norma dellart4 comma 10 bis del decreto legge 24 gennaio 2015n3");
        stopwords.add("STARTUP COSTITUITA A NORMA DELL'ART. 4 COMMA 10 BIS DEL DECRETO LEGGE 24 GENNAIO 2015, N. 3".toLowerCase());

        stopwords.add("start-up costituita a norma dell'art. 4 comma 10 bis del decreto legge 24 gennaio 2015, n. 3");

        stopwords.add("societa' a responsabilita' limitata");
        stopwords.add("societa a responsabilita limitata");
        stopwords.add("societ? a responsabilit? limitata");
        stopwords.add("s.r.l.");
        stopwords.add("s.r.l");
        stopwords.add("gmbh");
        stopwords.add(" sas");
        stopwords.add(", ");
        stopwords.add(". ");
        stopwords.add("-");
        stopwords.add("\"");


        List<String> stopwordsRegex = new ArrayList<>();
        stopwordsRegex.add("\\(\\D*\\)");

        for (Record r1 : c) {

            boolean debug = false;
            if (r1.getValue(Organization.ID).equals("32067")) {

                System.out.println("DEBUG");
                debug = true;
            }


        //    r1.setValue(Organization.LINK, u.clean(r1.getValue(Organization.LINK)));


            r1.setValue(Organization.ADDRESS, a.clean(r1.getValue(Organization.ADDRESS)));
//            r1.setValue(Organization.LABEL, l.clean(r1.getValue(Organization.LABEL)));

            String label = r1.getValue(Organization.LABEL);
//            System.out.println("OLD LABEL: "+ label);
            if (label != null) {
                label = label.toLowerCase();

                if (debug) {
                    System.out.println("INITIAL LABEL: " + label);
                }


                for (String stopword : stopwords) {

                    if (debug) {
                        System.out.println("Stopword: " + stopword + "\t\t" + label);
                    }

                    label = label.replace(stopword, "").trim().replaceAll(" +", " ");

                }

                for (String stopword : stopwordsSpace) {

                    if (debug) {
                        System.out.println("Stopword space: " + stopword + "\t\t" + label);
                    }

                    label = label.replace(stopword, " ").trim().replaceAll(" +", " ");

                }

                for (int i = 0; i < replace.size(); i++) {
                    String left = replace.get(i);
                    String right = substitution.get(i);

                    label = label.replace(left, right).trim().replaceAll(" +", " ");

                }

                for (String reg : stopwordsRegex) {

                    if (debug) {
                        System.out.println("reg: " + reg + "\t\t" + label);
                    }

                    label = label.replaceAll(reg, "");
                }

                if (debug) {
                    System.out.println("FINAL LABEL: " + label);
                }
//                System.out.println("NEW LABEL: "+ label);
            }
            r1.setValue(Organization.LABEL, label);


            String city = r1.getValue(Organization.CITY);
            if (city != null) {

                city = city.replaceAll("\\(.*\\)", "");
                for (String reg : stopwordsRegex) {
                    city = city.replaceAll(reg, "");
                }
//                System.out.println("City: " + city + " \t CITY: " + city);
            } else {
                //System.out.println("LABEL: " + r1.getValue(Organization.LABEL) + " NULL CITY");
            }


            r1.setValue(Organization.CITY, city);
        }


    }


}
