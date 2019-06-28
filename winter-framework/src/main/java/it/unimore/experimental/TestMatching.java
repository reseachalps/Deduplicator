package it.unimore.experimental;

import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.StandardRecordBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.rules.LinearCombinationMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.CSVRecordReader;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.comparators.RecordComparatorLevenshtein;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinEditDistance;
import it.unimore.cleaning.DBCleaner;
import it.unimore.comparators.RecordComparatorOrganization;
import it.unimore.deduplication.*;
import it.unimore.deduplication.model.Organization;
import org.apache.jena.base.Sys;
import org.apache.lucene.queryparser.flexible.standard.processors.OpenRangeQueryNodeProcessor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


public class TestMatching {

    public static void main(String arg[]) {
        boolean TEST = false;
        System.out.println("Start!");

        // DATASETS
        DataSet<Record, Attribute> openaire = new HashedDataSet<>();
        DataSet<Record, Attribute> cercauniversita = new HashedDataSet<>();
        DataSet<Record, Attribute> arianna = new HashedDataSet<>();
        DataSet<Record, Attribute> questio = new HashedDataSet<>();
        DataSet<Record, Attribute> bvd = new HashedDataSet<>();
        DataSet<Record, Attribute> cnr = new HashedDataSet<>();
        DataSet<Record, Attribute> scanr = new HashedDataSet<>();
        DataSet<Record, Attribute> registroImprese = new HashedDataSet<>();
        DataSet<Record, Attribute> patiris = new HashedDataSet<>();
        DataSet<Record, Attribute> orcid = new HashedDataSet<>();
        DataSet<Record, Attribute> orcidParent = new HashedDataSet<>();
        DataSet<Record, Attribute> all = new HashedDataSet<>();

        DataSet<Record, Attribute> p3 = new HashedDataSet<>();
        DataSet<Record, Attribute> grid = new HashedDataSet<>();


        DataSet<Record, Attribute> fwf = new HashedDataSet<>();
        DataSet<Record, Attribute> sicris = new HashedDataSet<>();
        // /Users/paolosottovia/Downloads/extracted_data_17_03_2018/

//        String basePath = "/Users/paolosottovia/Downloads/extracted_data_17_03_2018/";

        String basePath = arg[0];
//        String mode = arg[1];

        System.out.println("BASEPATH: " + basePath);
//        System.out.println("MODE: " + mode);


        boolean includeOrcid = true;


        DBCleaner cleaner = new OrganizationDBCleaner();


        File cercaUniversitaFile = new File(basePath + "organizations_CercaUniversita.csv");

        File openaireFile = new File(basePath + "organizations_OpenAire.csv");

        File questioFile = new File(basePath + "organizations_Questio.csv");

        File ariannaFile = new File(basePath + "organizations_Arianna - Anagrafe Nazionale delle Ricerche.csv");

        File dvbFile = new File(basePath + "organizations_Crawled.csv");

        File cnrFile = new File(basePath + "organizations_Consiglio Nazionale delle Ricerche (CNR).csv");

        File scanrFile = new File(basePath + "organizations_ScanR.csv");

        File registroImpreseFile = new File(basePath + "organizations_Startup - Registro delle imprese.csv");

        File patirisFile = new File(basePath + "organizations_Patiris.csv");

        File orcidFile = new File(basePath + "organizations_ORCID.csv");
        File orcidParentFile = new File(basePath + "organizations_ORCID_parent.csv");


        File p3File = new File(basePath + "organizations_P3.csv");

        File gridFile = new File(basePath + "organizations_Grid.csv");


        File fwfFile = new File(basePath + "organizations_Austrian Science Fund (FWF).csv");

        File sicrisFile = new File(basePath + "organizations_SICRIS - Slovenian Current Research Information System.csv");

        //read data from csv Files
        try {
            new CSVRecordReader(-1).loadFromCSV(
                    cercaUniversitaFile,
                    cercauniversita);
            new CSVRecordReader(-1).loadFromCSV(
                    openaireFile, openaire);

            new CSVRecordReader(-1).loadFromCSV(
                    questioFile, questio);

            new CSVRecordReader(-1).loadFromCSV(
                    ariannaFile, arianna);

            new CSVRecordReader(-1).loadFromCSV(
                    dvbFile, bvd);

            new CSVRecordReader(-1).loadFromCSV(
                    cnrFile, cnr);

            new CSVRecordReader(-1).loadFromCSV(
                    scanrFile, scanr);
            new CSVRecordReader(-1).loadFromCSV(
                    registroImpreseFile, registroImprese);

            new CSVRecordReader(-1).loadFromCSV(
                    patirisFile, patiris);

            if (includeOrcid) {

                new CSVRecordReader(-1).loadFromCSV(
                        orcidFile, orcid);


                new CSVRecordReader(-1).loadFromCSV(
                        orcidParentFile, orcidParent);
            }


            new CSVRecordReader(-1).loadFromCSV(
                    p3File, p3);

            new CSVRecordReader(-1).loadFromCSV(
                    gridFile, grid);

            new CSVRecordReader(-1).loadFromCSV(
                    fwfFile, fwf);

            new CSVRecordReader(-1).loadFromCSV(
                    sicrisFile, sicris);

        } catch (IOException e) {
            e.printStackTrace();


        }


        Map<String, Record> idRecordDuplicated = new HashMap<>();


        List<DataSet<Record, Attribute>> datasetDuplicated = new ArrayList<>();
        datasetDuplicated.add(openaire);

        if (includeOrcid) {
            datasetDuplicated.add(orcid);
        }


        for (DataSet<Record, Attribute> dataset : datasetDuplicated) {

            for (Record record : dataset.get()) {
                String id = record.getValue(Organization.ID);
                idRecordDuplicated.put(id, record);
            }

        }


        OrganizationSingleDatasetResolution OSDR = new OrganizationSingleDatasetResolution();

        cleaner.cleanDB(openaire);

        List<Set<String>> matchesOpenAire = OSDR.runIdentityResolution(openaire);

        System.out.println("Number of OPENAIRE matches: " + matchesOpenAire.size());


        Map<String, Set<String>> candidateOthersOpenAire = new HashMap<>();

        Set<String> openaireIdToRemove = new HashSet<>();
        Set<String> openaireIdToMantain = new HashSet<>();

        System.out.println("OPEN AIRE CANDIDATE SELECTION ");

        for (Set<String> match : matchesOpenAire) {
            List<String> m = new ArrayList<>();
            m.addAll(match);


            Collections.sort(m, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    Record r1 = idRecordDuplicated.get(o1);
                    Record r2 = idRecordDuplicated.get(o2);


                    int n1 = OrganizationDeduplication.getNullValues(r1);
                    int n2 = OrganizationDeduplication.getNullValues(r2);

                    System.out.println("ID: " + o1 + "\t" + n1);
                    System.out.println("ID: " + o2 + "\t" + n2);


                    return Integer.compare(n1, n2);
                }
            });

            String candidate = m.get(0);
            openaireIdToMantain.add(candidate);
            Set<String> others = new HashSet<>();
            for (int i = 1; i < m.size(); i++) {
                others.add(m.get(i));
            }
            openaireIdToRemove.addAll(others);


            System.out.println("CANDIDATE: " + candidate + "\tOTHERS: " + others.toString());


            candidateOthersOpenAire.put(candidate, others);

        }


        DataSet<Record, Attribute> openAireFiltered = OrganizationDeduplication.filterDatasetByIds(openaire, openaireIdToRemove);

        System.out.println("OPENAIRE FILTERED: " + openAireFiltered.size());


        Collection<Correspondence<Record, Attribute>> allCorrespondances = new ArrayList<>();

        StandardRecordBlocker<Record, Attribute> blocker = new StandardRecordBlocker<>(new OrganizationBlockingLabelQGrams());


        LinearCombinationMatchingRule<Record, Attribute> matchingRule = new LinearCombinationMatchingRule<>(0.85);
        RecordComparatorOrganization organizationComparator = new RecordComparatorOrganization(Organization.LABEL, Organization.LABEL);
        try {
            matchingRule.addComparator(organizationComparator, 1.0d);
        } catch (Exception e) {
            e.printStackTrace();
        }


        List<DataSet> dataSets = new ArrayList<>();

        DataSet<Record, Attribute> openaireFR = OrganizationDeduplication.filterDatasetByCountryCode(openAireFiltered, "FR");

        dataSets.add(openaireFR);
        dataSets.add(scanr);


        Map<String, Record> idRecords = new HashMap<>();
        for (DataSet<Record, Attribute> d : dataSets) {
            for (Record r : d.get()) {
                String id = r.getValue(Organization.ID);
                idRecords.put(id, r);
            }

        }

        Attribute LABEL = new Attribute("label");
        Attribute ADDRESS = new Attribute("address");
        Attribute CITY = new Attribute("city");
        Attribute LINKS = new Attribute("links");

        for (int i = 0; i < dataSets.size(); i++) {
            for (int j = i; j < dataSets.size(); j++) {

                if (i == j) {
                    continue;
                }


                DataSet<Record, Attribute> dataSet1 = dataSets.get(i);
                DataSet<Record, Attribute> dataSet2 = dataSets.get(j);

                // skip not useful comparison
                if ((dataSet1.size() == 0) || (dataSet2.size() == 0)) {
                    continue;
                }
                System.out.println("Dataset1 size: " + dataSet1.size());
                System.out.println("Dataset2 size: " + dataSet2.size());

                MatchingEngine<Record, Attribute> engine = new MatchingEngine<>();

                Processable<Correspondence<Record, Attribute>> correspondences = engine.runIdentityResolution(dataSet1,
                        dataSet2, null, matchingRule, blocker);


                System.out.println("Number of correspondences: " + correspondences.size());


                Collection<Correspondence<Record, Attribute>> corresp = correspondences.get();
                allCorrespondances.addAll(corresp);


                for (Correspondence<Record, Attribute> corr : corresp) {
                    Record record1 = corr.getFirstRecord();
                    Record record2 = corr.getSecondRecord();

                    double sim = corr.getSimilarityScore();

                    System.out.println("\n\n--------------------------------------------------------------------");
                    System.out.println("R1: " + record1.toString());
                    System.out.println("R2: " + record2.toString());
                    System.out.println("score: " + sim);

                    System.out.println("LABEL\tADDRESS\tCITY\tLINKS");
                    System.out.println(record1.getValue(Organization.ID) + "\t" + record1.getValue(LABEL) + "\t" + record1.getValue(ADDRESS) + "\t" + record1.getValue(CITY) + "\t" + record1.getValue(LINKS) + "\t" + idRecords.get(record1.getValue(Organization.ID)));
                    System.out.println(record2.getValue(Organization.ID) + "\t" + record2.getValue(LABEL) + "\t" + record2.getValue(ADDRESS) + "\t" + record2.getValue(CITY) + "\t" + record2.getValue(LINKS) + "\t" + idRecords.get(record2.getValue(Organization.ID)));

                }


                System.out.println("\n\nNumber of step correspondences: " + correspondences.size());

            }
        }


        System.out.println("\n\n\n NUMBER of ALL correspondences: " + allCorrespondances.size());

        OrganizationDeduplication.writeMatches("matches_minimal.tsv", allCorrespondances);


        Set<String> matchedIds = new HashSet<>();

        Set<Set<String>> matches = new HashSet<>();
        for (Correspondence<Record, Attribute> a : allCorrespondances) {

            Record r1 = a.getFirstRecord();
            Record r2 = a.getSecondRecord();

            matchedIds.add(r1.getValue(Organization.ID));
            matchedIds.add(r2.getValue(Organization.ID));


            Set<String> elem = new HashSet<>();
            elem.add(r1.getValue(Organization.ID));
            elem.add(r2.getValue(Organization.ID));

            matches.add(elem);


        }


        List<Record> recordsNotMatched = new ArrayList<>();

        for (Record r : openaireFR.get()) {

            String id = r.getValue(Organization.ID);
            if (!matchedIds.contains(id)) {
                recordsNotMatched.add(r);
            }

        }


        System.out.println("RECORD NOT MATCHED: ");

        for (Record record1 : recordsNotMatched) {
            System.out.println(record1.getValue(Organization.ID) + "\t" + record1.getValue(LABEL) + "\t" + record1.getValue(ADDRESS) + "\t" + record1.getValue(CITY) + "\t" + record1.getValue(LINKS));

        }


//       Map<Set<String>,Double> matchingPairs = generateAllMatchingPairs(dataSets, Organization.LABEL, 0.0d);


//        double[] thresholds = {0.9d, 0.8d, 0.7d, 0.6d, 0.5d, 0.4d, 0.3d, 0.2d, 0.1d};

        String[] attributesNames = {"LABEL", "CITY", "ADDRESS", "LINK"};
        Attribute[] attributes = {Organization.LABEL, Organization.CITY, Organization.ADDRESS, Organization.LINK};


        Map<Attribute, Map<Set<String>, Double>> attributeRuns = new HashMap<>();
        for (Attribute attribute : attributes) {


            Map<Set<String>, Double> partialMatches = generateAllMatchingPairs(dataSets, attribute, -0.01d);

            System.out.println("Attribute: " + attribute.getName() + " number of matches: " + partialMatches.size());

            attributeRuns.put(attribute, partialMatches);


        }


        System.out.println("DONE!!");


        BufferedWriter writer = null;

        BufferedWriter writer1 = null;


//        System.out.println("Number of edges: " + edges.size());
        try {
            writer = new BufferedWriter(new FileWriter("decision_tree_data.tsv"));


            writer1 = new BufferedWriter(new FileWriter("matches_deep_learning.csv"));

            List<Set<String>> keys = new ArrayList<>();
            keys.addAll(attributeRuns.get(attributes[0]).keySet());


            String line = "";
            for (String attName : attributesNames) {
                line += attName + "\t";

            }
            line = line + "MATCH";
            line = line + "\n";

            writer.write(line);
            writer1.write("ltable_id,rtable_id,label\n");

            for (Set<String> entry : keys) {

                String li = "";
                String li2 = "";
                for (Attribute att : attributes) {


                    double sim = 0.0d;

                    if (attributeRuns.containsKey(att)) {
                        if (attributeRuns.get(att).containsKey(entry)) {
                            sim = attributeRuns.get(att).get(entry);
                        } else {
                            System.err.println("PAIR " + entry.toString() + " : " + att.getName() + " is not present");
                        }
                    } else {
                        System.err.println("ERROR EMPTY ATTRIBUTE : " + att.getName());
                    }


                    li = li + sim + "\t";

                }


                List<String> pairList = new ArrayList<>();
                pairList.addAll(entry);


                for (String p : pairList) {
                    li2 += p + ",";
                }


                if (matches.contains(entry)) {

                    li2 += 1;
                    li += 1;
                } else {
                    li2 += 0;
                    li += 0;
                }
                li += "\n";


                writer.write(li);
                writer1.write(li2);


            }

            writer.close();
            writer1.close();

        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    public static Map<Set<String>, Double> generateAllMatchingPairs(List<DataSet> dataSets, Attribute attribute, double THRESHOLD) {

        Collection<Correspondence<Record, Attribute>> correspondeces = new ArrayList<>();
        StandardRecordBlocker<Record, Attribute> blocker = new StandardRecordBlocker<>(new OrganizationBlockingLabelQGrams());
        LinearCombinationMatchingRule<Record, Attribute> matchingRule = new LinearCombinationMatchingRule<>(THRESHOLD);

        Map<Set<String>, Double> pairScores = new HashMap<>();
        RecordComparatorLevenshtein levenshtein = new RecordComparatorLevenshtein(attribute, attribute);
        levenshtein.setLowerCase(true);


        try {
            matchingRule.addComparator(levenshtein, 1.0d);
        } catch (Exception e) {
            e.printStackTrace();
        }


        for (int i = 0; i < dataSets.size(); i++) {
            for (int j = i; j < dataSets.size(); j++) {

                if (i == j) {
                    continue;
                }


                DataSet<Record, Attribute> dataSet1 = dataSets.get(i);
                DataSet<Record, Attribute> dataSet2 = dataSets.get(j);

                // skip not useful comparison
                if ((dataSet1.size() == 0) || (dataSet2.size() == 0)) {
                    continue;
                }
                System.out.println("Dataset1 size: " + dataSet1.size());
                System.out.println("Dataset2 size: " + dataSet2.size());

                MatchingEngine<Record, Attribute> engine = new MatchingEngine<>();

                Processable<Correspondence<Record, Attribute>> correspondences = engine.runIdentityResolution(dataSet1,
                        dataSet2, null, matchingRule, blocker);


                System.out.println("Number of correspondences: " + correspondences.size());


                Collection<Correspondence<Record, Attribute>> corresp = correspondences.get();
                correspondeces.addAll(corresp);

                System.out.println("\n\nNumber of step correspondences: " + correspondences.size());

            }
        }


        Set<Set<String>> pairs = new HashSet<>();


        for (Correspondence<Record, Attribute> corr : correspondeces) {


            String id1 = corr.getFirstRecord().getValue(Organization.ID);
            String id2 = corr.getSecondRecord().getValue(Organization.ID);
            double score = corr.getSimilarityScore();


            Set<String> pair = new HashSet<>();
            pair.add(id1);
            pair.add(id2);

            pairs.add(pair);


            pairScores.put(pair, score);


        }


        System.out.println("Number of pairs: " + pairs.size());


        return pairScores;


    }


}
