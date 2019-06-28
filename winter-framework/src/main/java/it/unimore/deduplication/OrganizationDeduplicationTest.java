package it.unimore.deduplication;

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
import it.unimore.comparators.RecordComparatorOrganization;
import it.unimore.deduplication.model.Organization;
import org.jgrapht.Graph;
import org.jgrapht.UndirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


public class OrganizationDeduplicationTest {


    public static void main(String arg[]) {
        System.out.println("Start!");

        // DATASETS
//        DataSet<Record, Attribute> openaire = new HashedDataSet<>();
        DataSet<Record, Attribute> cercauniversita = new HashedDataSet<>();
//        DataSet<Record, Attribute> arianna = new HashedDataSet<>();
        DataSet<Record, Attribute> questio = new HashedDataSet<>();
//        DataSet<Record, Attribute> bvd = new HashedDataSet<>();
//        DataSet<Record, Attribute> cnr = new HashedDataSet<>();
//        DataSet<Record, Attribute> scanr = new HashedDataSet<>();
//        DataSet<Record, Attribute> registroImprese = new HashedDataSet<>();
//        DataSet<Record, Attribute> patiris = new HashedDataSet<>();
//        DataSet<Record, Attribute> all = new HashedDataSet<>();

        // /Users/paolosottovia/Downloads/extracted_data_17_03_2018/

//        String basePath = "/Users/paolosottovia/Downloads/extracted_data_17_03_2018/";

        String basePath = arg[0];
        String mode = arg[1];

        System.out.println("BASEPATH: " + basePath);
        System.out.println("MODE: " + mode);


        File cercaUniversitaFile = new File(basePath + "organizations_CercaUniversita.csv");

        File openaireFile = new File(basePath + "organizations_OpenAire.csv");

        File questioFile = new File(basePath + "organizations_Questio.csv");

        File ariannaFile = new File(basePath + "organizations_Arianna - Anagrafe Nazionale delle Ricerche.csv");

        File dvbFile = new File(basePath + "organizations_Bureau van Dijk.csv");

        File cnrFile = new File(basePath + "organizations_Consiglio Nazionale delle Ricerche (CNR).csv");

        File scanrFile = new File(basePath + "organizations_ScanR.csv");

        File registroImpreseFile = new File(basePath + "organizations_Startup - Registro delle imprese.csv");

        File patirisFile = new File(basePath + "organizations_Patiris.csv");


        //read data from csv Files
        try {
            new CSVRecordReader(-1).loadFromCSV(
                    cercaUniversitaFile,
                    cercauniversita);
//            new CSVRecordReader(-1).loadFromCSV(
//                    openaireFile, openaire);

            new CSVRecordReader(-1).loadFromCSV(
                    questioFile, questio);

//            new CSVRecordReader(-1).loadFromCSV(
//                    ariannaFile, arianna);

//            new CSVRecordReader(-1).loadFromCSV(
//                    dvbFile, bvd);
//
//            new CSVRecordReader(-1).loadFromCSV(
//                    cnrFile, cnr);
//
//            new CSVRecordReader(-1).loadFromCSV(
//                    scanrFile, scanr);
//            new CSVRecordReader(-1).loadFromCSV(
//                    registroImpreseFile, registroImprese);
//
//            new CSVRecordReader(-1).loadFromCSV(
//                    patirisFile, patiris);

        } catch (IOException e) {
            e.printStackTrace();
        }

//        System.out.println("Number of items in Openaire: " + openaire.size());
        System.out.println("Number of items in cerca universita: " + cercauniversita.size());
        System.out.println("Number of items in questio: " + questio.size());
//        System.out.println("Number of items in arianna: " + arianna.size());
//        System.out.println("Number of items in bvd: " + bvd.size());
//        System.out.println("Number of items in cnr: " + bvd.size());
//        System.out.println("Number of items in scanr: " + scanr.size());
//        System.out.println("Number of items in registro imprese: " + registroImprese.size());
//        System.out.println("Number of items in patiris: " + patiris.size());
        List<List<DataSet<Record, Attribute>>> ALL_DATASETS = new ArrayList<>();


        //Cleaner
        OrganizationDBCleaner cleaner = new OrganizationDBCleaner();
//        cleaner.cleanDB(openaire);
        cleaner.cleanDB(cercauniversita);
        cleaner.cleanDB(questio);
//        cleaner.cleanDB(arianna);
//        cleaner.cleanDB(bvd);
//        cleaner.cleanDB(cnr);
//        cleaner.cleanDB(openaire);
//        cleaner.cleanDB(scanr);
//        cleaner.cleanDB(registroImprese);
//        cleaner.cleanDB(patiris);

        List<DataSet<Record, Attribute>> datasetsIT = new ArrayList<>();
//        List<DataSet<Record, Attribute>> datasetsFR = new ArrayList<>();
//        datasetsIT.add(openaire);
        datasetsIT.add(cercauniversita);
        datasetsIT.add(questio);
//        datasetsIT.add(arianna);
//        datasetsIT.add(bvd);
//        datasetsIT.add(cnr);
//        datasetsIT.add(registroImprese);
//        datasetsIT.add(patiris);

//        datasetsFR.add(openaire);
//        datasetsFR.add(scanr);

        ALL_DATASETS.add(datasetsIT);


        Attribute labelOpenAire = new Attribute("label");
        Attribute labelCercaUniversita = new Attribute("label");

        LinearCombinationMatchingRule<Record, Attribute> matchingRule = new LinearCombinationMatchingRule<>(0.7);
        // add comparators


        StandardRecordBlocker<Record, Attribute> blocker = new StandardRecordBlocker<>(new OrganizationBlockingKey());

        switch (mode) {

            case "normal":
                blocker = new StandardRecordBlocker<>(new OrganizationBlockingKey());

                break;
            case "city":
                blocker = new StandardRecordBlocker<>(new OrganizationBlockingCity());

                break;
            case "cityQgrams":
                blocker = new StandardRecordBlocker<>(new OrganizationBlockingCityQGrams());

                break;


            case "labelQgrams":
                blocker = new StandardRecordBlocker<>(new OrganizationBlockingLabelQGrams());

                break;
            default:
                System.exit(0);
        }


        Graph<String, DefaultEdge> g
                = new SimpleGraph<>(DefaultEdge.class);


//        OrganizationSingleDatasetResolution OSDR = new OrganizationSingleDatasetResolution();
//
//        cleaner.cleanDB(openaire);
//
//        List<Set<String>> matchesOpenAire = OSDR.runIdentityResolution(openaire);
//        for (Set<String> m : matchesOpenAire) {
//
//            List<String> ids = new ArrayList<>();
//            ids.addAll(m);
//
//
//            g.addVertex(ids.get(0));
//            g.addVertex(ids.get(1));
//            g.addEdge(ids.get(0), ids.get(1));
//        }


        try {

            // RecordComparatorLevenshtein lab = new
            // RecordComparatorLevenshtein(Organization.LABEL, Organization.LABEL);
            //RecordComparatorLevenshtein lab = new RecordComparatorLevenshtein(labelOpenAire, labelCercaUniversita);
            RecordComparatorLevenshtein lab = new RecordComparatorLevenshtein(labelOpenAire, labelCercaUniversita);
            lab.setLowerCase(true);
//            matchingRule.addComparator(lab, 0.6d);
            //


            RecordComparatorLevenshtein city = new RecordComparatorLevenshtein(Organization.CITY, Organization.CITY);
            city.setLowerCase(true);
//            matchingRule.addComparator(city, 0.2d);


            RecordComparatorLevenshtein link = new RecordComparatorLevenshtein(Organization.LINK, Organization.LINK);
            link.setLowerCase(true);


            RecordComparatorOrganization organizationComparator = new RecordComparatorOrganization(Organization.LABEL, Organization.LABEL);

            matchingRule.addComparator(organizationComparator, 1.0d);


//          matchingRule.addComparator(link, 0.2d);
//			RecordComparatorLevenshtein url = new RecordComparatorLevenshtein(Organization.LINK, Organization.LINK);
//			matchingRule.addComparator(url, 0.3d);


            // Initialize Matching Engine
            Collection<Correspondence<Record, Attribute>> allCorrespondances = new ArrayList<>();


            Attribute LABEL = new Attribute("label");
            Attribute ADDRESS = new Attribute("address");
            Attribute CITY = new Attribute("city");
            Attribute LINKS = new Attribute("links");

            for (List<DataSet<Record, Attribute>> allDatasets : ALL_DATASETS) {

                for (int i = 0; i < allDatasets.size(); i++) {
                    for (int j = i; j < allDatasets.size(); j++) {

                        if (i == j) {
                            continue;
                        }

                        DataSet<Record, Attribute> dataSet1 = allDatasets.get(i);
                        DataSet<Record, Attribute> dataSet2 = allDatasets.get(j);

                        System.out.println("Dataset1 size: " + dataSet1.size());
                        System.out.println("Dataset2 size: " + dataSet2.size());

                        MatchingEngine<Record, Attribute> engine = new MatchingEngine<>();
//                    Processable<Correspondence<Record, Attribute>> correspondences = engine.runIdentityResolution(openaire,
//                            bvd, null, matchingRule, blocker);

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
                            System.out.println(record1.getValue(LABEL) + "\t" + record1.getValue(ADDRESS) + "\t" + record1.getValue(CITY) + "\t" + record1.getValue(LINKS));
                            System.out.println(record2.getValue(LABEL) + "\t" + record2.getValue(ADDRESS) + "\t" + record2.getValue(CITY) + "\t" + record2.getValue(LINKS));

                        }


                        System.out.println("\n\nNumber of step correspondences: " + correspondences.size());

                    }
                }


            }

            System.out.println("\n\n\n NUMBER of ALL correspondences: " + allCorrespondances.size());


            Map<String, Set<Record>> ids = new HashMap<>();


            Set<Set<String>> connectedComponents = new HashSet<>();

            Map<String, Record> idRecords = new HashMap<>();
            Attribute attributeId = new Attribute("id");

            Set<String> idsOpenAire = new HashSet<>();
//            for (Set<String> matches : matchesOpenAire) {
//                idsOpenAire.addAll(matches);
//            }
//
//            for (Record rOA : openaire.get()) {
//                if (idsOpenAire.contains(rOA.getValue(attributeId))) {
//                    idRecords.put(rOA.getValue(attributeId), rOA);
//                }
//            }


            Set<String> allIds = new HashSet<>();

            //Given all the correspondences create a graph
            //Nodes are the entities
            //Edges are the correspondences

            for (Correspondence<Record, Attribute> corr : allCorrespondances) {

                Record r1 = corr.getFirstRecord();
                Record r2 = corr.getSecondRecord();

                String id1 = r1.getValue(attributeId);
                String id2 = r2.getValue(attributeId);

                g.addVertex(id1);
                g.addVertex(id2);
                g.addEdge(id1, id2);


                allIds.add(id1);
                allIds.add(id2);

                idRecords.put(id1, r1);
                idRecords.put(id2, r2);

                boolean inserted = false;


                for (Set<String> component : connectedComponents) {

                    if (component.contains(id1) || component.contains(id2)) {
                        component.add(id1);
                        component.add(id2);
                        inserted = true;
                        break;
                    }
                }

                if (!inserted) {
                    Set<String> idSet = new HashSet<>();
                    idSet.add(id1);
                    idSet.add(id2);
                    connectedComponents.add(idSet);
                }


            }


            System.out.println("Number of edges: " + g.edgeSet().size());
            System.out.println("Number of nodes: " + g.vertexSet().size());
            ConnectivityInspector<String, DefaultEdge> c = new ConnectivityInspector<String, DefaultEdge>((UndirectedGraph<String, DefaultEdge>) g);

            //Connected components represent the set of entities that represent the same real-wold entity
            List<Set<String>> connectedComponentsCleaned = c.connectedSets();
            System.out.println("Number of connected components: " + connectedComponentsCleaned.size());


            System.out.println("MATCH RESULTS: ");


            Collections.sort(connectedComponentsCleaned, new Comparator<Set<String>>() {
                @Override
                public int compare(Set<String> o1, Set<String> o2) {
                    return -1 * Integer.compare(o1.size(), o2.size());
                }
            });

            int i = 0;
            for (Set<String> set : connectedComponentsCleaned) {
                i++;
                System.out.println("\n\n\n===============================================================");
                System.out.println("SIZE: " + set.size());
                System.out.println("LABEL\tADDRESS\tCITY\tLINKS");

                for (String id : set) {
                    Record record = idRecords.get(id);
                    System.out.println(record.getValue(LABEL) + "\t" + record.getValue(ADDRESS) + "\t" + record.getValue(CITY) + "\t" + record.getValue(LINKS));

                }


            }
//            writeCorrespondence("correspondenceOrganizations_" + mode + ".tsv", connectedComponentsCleaned);


            System.out.println("Founded " + i + " correspondences.");

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.exit(0);

        }


    }


    public static void writeCorrespondence(String filename, Collection<Set<String>> correspondence) {
        BufferedWriter writer = null;
        try {

            File logFile = new File(filename);

            // This will output the full path where the file will be written to...
            System.out.println(logFile.getCanonicalPath());

            writer = new BufferedWriter(new FileWriter(logFile));

            for (Set<String> corr : correspondence) {
                String line = "";

                for (String c : corr) {
                    line += c + "\t";
                }
                line = line.substring(0, line.length() - 1);

                writer.write(line + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                // Close the writer regardless of what happens...
                writer.close();
            } catch (Exception e) {
            }
        }

    }
}
