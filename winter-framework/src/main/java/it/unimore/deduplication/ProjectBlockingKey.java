package it.unimore.deduplication;

import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.RecordBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import it.unimore.deduplication.model.Project;

import java.util.ArrayList;
import java.util.List;

public class ProjectBlockingKey extends RecordBlockingKeyGenerator<Record, Attribute> {
    @Override
    public void generateBlockingKeys(Record record, Processable<Correspondence<Attribute, Matchable>> correspondences, DataIterator<Pair<String, Record>> resultCollector) {

//        String year = null;
//
//        if (record.hasValue(Project.YEAR)) {
//            year = record.getValue(Project.YEAR);
////            System.out.println("Case 1: " + year);
//
//        } else {
////            System.out.println("Case 2: ");
//            if (record.hasValue(Project.START_DATE)) {
////                System.out.println("Case 2.1 ");
//                String date = record.getValue(Project.START_DATE);
//                String[] comp = date.split(" ");
//                if (comp.length >= 1) {
//                    year = comp[comp.length - 1];
//                } else {
//                    System.out.println("Error startdate: " + date);
//                    year = "XXXX";
//                }
//            } else {
////                System.out.println("Case 2.2 ");
////                System.out.println("Record: " + record.toString());
//                year = "XXXX";
//            }
//        }
//
//
//        if (year.length() != 4) {
//            System.err.println("ERROR into year field: " + year);
//        }
//
////        System.out.println("Key: "+ year + " record: "+ record.toString());


        String label = null;


        List<String> stopwords = new ArrayList<>();
        stopwords.add("the");

        stopwords.add("of");
        stopwords.add("and");
        stopwords.add("for");
        stopwords.add("in");
        stopwords.add("the");
        stopwords.add("di");
        stopwords.add("la");
        stopwords.add("et");
        stopwords.add("to");
        stopwords.add("a");
        stopwords.add("e");
        stopwords.add("per");
        stopwords.add("on");
        stopwords.add("de");

        stopwords.add("et");//	14621
        stopwords.add("des");//14327
        stopwords.add("la");//	13330
        stopwords.add("di");//	11824
        stopwords.add("und");//	11530
        stopwords.add("to");   //10714
        stopwords.add("a");    ///9533
        stopwords.add("der");    //8579
        stopwords.add("on");//7361
        stopwords.add("-");
        //7328
        stopwords.add("The"); //6046"):
        stopwords.add("e");//5943"):
        stopwords.add("du"); // 5715"):
        stopwords.add("with"); // 5256"):
        stopwords.add("en"); // 4733
        stopwords.add("dans"); // 4721
        stopwords.add("à"); // 4496
        stopwords.add("von"); // 4351
        stopwords.add("per"); // 4119
        stopwords.add("A");// 4076
        stopwords.add("les"); // 4023
        stopwords.add("by"); // 4016
        stopwords.add("le");
        stopwords.add("pour"); // 3554

        stopwords.add("from"); //

        if (record.hasValue(Project.LABEL)) {
            label = record.getValue(Project.LABEL);
        }

        if (label != null) {

            label = label.replaceAll("\\(.*\\)", "").trim();
            label = label.toLowerCase();
//            int Q = 3;
//
//            List<String> qgrams = new ArrayList<>();
//            for (int i = 0; i < label.length() - Q; i++) {
//                String qgram = label.substring(i, i + Q);
//                qgrams.add(qgram);
//            }
//            for (String qgram : qgrams) {
//                resultCollector.next(new Pair<>((qgram), record));
//            }


            String[] words = label.split(" ");
            for (String word : words) {
                if (!stopwords.contains(word)) {
                    resultCollector.next(new Pair<>((word), record));
                }
            }

        }


    }
}
