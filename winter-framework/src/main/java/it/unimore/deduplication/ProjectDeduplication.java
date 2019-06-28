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
import it.unimore.deduplication.model.Organization;
import it.unimore.deduplication.model.Project;
import org.jgrapht.Graph;
import org.jgrapht.UndirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.Pseudograph;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class ProjectDeduplication {
    public static void main(String arg[]) {


        DataSet<Record, Attribute> openaire = new HashedDataSet<>();
        DataSet<Record, Attribute> arianna = new HashedDataSet<>();

        DataSet<Record, Attribute> scanr = new HashedDataSet<>();
        DataSet<Record, Attribute> questio = new HashedDataSet<>();

        DataSet<Record,Attribute> p3 = new HashedDataSet<>();

        DataSet<Record,Attribute> grid = new HashedDataSet<>();

        DataSet<Record,Attribute> fwf = new HashedDataSet<>();

        DataSet<Record,Attribute> sicris = new HashedDataSet<>();
        DataSet<Record, Attribute> all = new HashedDataSet<>();


        String basePath = arg
                [0];


        ///Users/paolosottovia/Downloads/extracted_data_09_03_2018/

        File ariannaFile = new File(basePath + "projects_Arianna - Anagrafe Nazionale delle Ricerche.csv");

        File openaireFile = new File(basePath + "projects_OpenAire.csv");


        File scanrFile = new File(basePath + "projects_ScanR.csv");


        File questioFile = new File(basePath + "projects_Questio.csv");


        File p3File = new File(basePath + "projects_P3.csv");
        File gridFile = new File(basePath + "projects_Grid.csv");

        File fwfFile = new File(basePath + "projects_Austrian Science Fund (FWF).csv");

        File  sicrisFile = new File("projects_SICRIS - Slovenian Current Research Information System.csv");

        //read data from csv Files
        try {

            new CSVRecordReader(-1).loadFromCSV(
                    openaireFile, openaire);

            new CSVRecordReader(-1).loadFromCSV(
                    scanrFile, scanr);

            new CSVRecordReader(-1).loadFromCSV(
                    ariannaFile, arianna);

            new CSVRecordReader(-1).loadFromCSV(
                    questioFile, questio);

            new CSVRecordReader(-1).loadFromCSV(
                    gridFile, grid);

            new CSVRecordReader(-1).loadFromCSV(
                    p3File, p3);

            new CSVRecordReader(-1).loadFromCSV(
                    fwfFile, fwf);

            new CSVRecordReader(-1).loadFromCSV(
                    sicrisFile, sicris);

        } catch (IOException e) {

            e.printStackTrace();
        }

        System.out.println("Number of items in Openaire: " + openaire.size());
        System.out.println("Number of items in scan r: " + scanr.size());
        System.out.println("Number of items in arianna: " + arianna.size());
        System.out.println("Number of items in questio: " + questio.size());
        System.out.println("Number of items in P3: " + p3.size());
        System.out.println("Number of items in GRID: " + grid.size());
        System.out.println("Number of items in fwf: " + fwf.size());
        System.out.println("Number of items in sicris: " + sicris.size());
        List<DataSet<Record, Attribute>> allDatasets = new ArrayList<>();

        allDatasets.add(openaire);

        allDatasets.add(arianna);
        allDatasets.add(questio);
        allDatasets.add(p3);
        allDatasets.add(grid);
        allDatasets.add(fwf);
        allDatasets.add(sicris);
        //just for quick debug
        allDatasets.add(scanr);

        Map<String, Integer> wordCount = new HashMap<>();

        for (DataSet<Record, Attribute> records : allDatasets) {


            for (Record r : records.get()) {

                String label = r.getValue(Project.LABEL);

                if (label != null) {

                    String words[]
                            = label.split(" ");

                    for (String word : words) {
                        if (wordCount.containsKey(word)) {
                            int count = wordCount.get(word);
                            wordCount.remove(word);
                            count = count + 1;
                            wordCount.put(word, count);

                        } else {
                            wordCount.put(word, 1);
                        }
                    }
                }

            }


        }


        List<Map.Entry<String, Integer>> list = new ArrayList<>(wordCount.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return -1 * o1.getValue().compareTo(o2.getValue());
            }
        });

        System.out.println("VALUES DISTRIBUTION: " + wordCount.size());
        for (Map.Entry<String, Integer> entry : list) {
            System.out.println(entry.getKey() + "\t" + entry.getValue());
        }


        LinearCombinationMatchingRule<Record, Attribute> matchingRule = new LinearCombinationMatchingRule<>(0.75);
        // add comparators


        StandardRecordBlocker<Record, Attribute> blocker = new StandardRecordBlocker<>(new ProjectBlockingKey());

        Graph<String, DefaultEdge> g
                = new Pseudograph<>(DefaultEdge.class);


        try {

            // RecordComparatorLevenshtein lab = new
            // RecordComparatorLevenshtein(Organization.LABEL, Organization.LABEL);
            RecordComparatorLevenshtein label = new RecordComparatorLevenshtein(Project.LABEL, Project.LABEL);
            label.setLowerCase(true);
            matchingRule.addComparator(label, 1.0d);
            //

            // Initialize Matching Engine
            Collection<Correspondence<Record, Attribute>> allCorrespondances = new ArrayList<>();


            Attribute LABEL = new Attribute("label");
            Attribute ACRONYM = new Attribute("acronym");
            Attribute YEAR = new Attribute("year");
            Attribute ORGS = new Attribute("orgs");

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

                        System.out.println("LABEL\tACRONYM\tYEAR\tORGS");
                        System.out.println(record1.getValue(LABEL) + "\t" + record1.getValue(ACRONYM) + "\t" + record1.getValue(YEAR) + "\t" + record1.getValue(ORGS));
                        System.out.println(record2.getValue(LABEL) + "\t" + record2.getValue(ACRONYM) + "\t" + record2.getValue(YEAR) + "\t" + record2.getValue(ORGS));

                    }


                    System.out.println("\n\nNumber of step correspondences: " + correspondences.size());

                }
            }


            System.out.println("\n\n\n NUMBER of ALL correspondences: " + allCorrespondances.size());

            //
            //
            //
            //
            //
            //
            //
            //
            //
            //
            //
            Set<Set<String>> connectedComponents = new HashSet<>();

            Map<String, Record> idRecords = new HashMap<>();
            Set<String> allIds = new HashSet<>();

            //Given all the correspondences create a graph
            //Nodes are the entities
            //Edges are the correspondences
            Attribute attributeId = new Attribute("id");

            System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n");

            for (Correspondence<Record, Attribute> corr : allCorrespondances) {

                Record r1 = corr.getFirstRecord();
                Record r2 = corr.getSecondRecord();

                String id1 = r1.getValue(attributeId);
                String id2 = r2.getValue(attributeId);

                g.addVertex(id1);
                g.addVertex(id2);

                System.out.println("Edge: " + id1 + "\t" + id2);

                if (g.containsEdge(id1, id2)) {
                    System.err.println("Duplicate!!!");

                } else {
                    g.addEdge(id1, id2);
                }


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


            for (Set<String> set : connectedComponentsCleaned) {

                System.out.println("\n\n\n===============================================================");
                System.out.println("LABEL\tADDRESS\tCITY\tLINKS");

                for (String id : set) {
                    Record record = idRecords.get(id);
                    System.out.println("LABEL\tACRONYM\tYEAR\tORGS");
                    System.out.println(record.getValue(LABEL) + "\t" + record.getValue(ACRONYM) + "\t" + record.getValue(YEAR) + "\t" + record.getValue(ORGS));
                }


            }
            printCorrespondence("correspondenceProjects.tsv", connectedComponentsCleaned);


        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    public static void printCorrespondence(String filename, Collection<Set<String>> correspondence) {
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


