package it.unimore.deduplication;

import org.simmetrics.metrics.Jaccard;
import weka.clusterers.EM;
import weka.clusterers.DensityBasedClusterer;

import java.util.HashSet;
import java.util.Set;

public class TestWeka {


    public static void main(String arg[]) {


        String s1 = "abc";
        String s2 = "abc";

        Set<String> gramsS1 = new HashSet<>();
        gramsS1.addAll(retrieve3Grams(s1));
        Set<String> gramsS2 = new HashSet<>();
        gramsS2.addAll(retrieve3Grams(s2));

        Jaccard<String> j = new Jaccard<>();
//        j

        System.out.println("JACCARD: "+ j.compare(gramsS1,gramsS2));







    }

    private static Set<String> retrieve3Grams(String label) {
        Set<String> qgrams = new HashSet<>();


        int Q = 3;


        for (int i = 0; i < label.length() - Q; i++) {
            String qgram = label.substring(i, i + Q);
            if (!qgram.contains(" ")) {
                qgrams.add(qgram);
            }
        }

        return qgrams;

    }
}
