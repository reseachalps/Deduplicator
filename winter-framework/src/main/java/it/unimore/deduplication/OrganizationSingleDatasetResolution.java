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
import it.unimore.cleaning.DBCleaner;
import it.unimore.comparators.RecordComparatorLabelLeveinstein;
import it.unimore.comparators.RecordComparatorOrganizationOpenAire;
import it.unimore.comparators.UrlComparator;
import it.unimore.deduplication.model.Organization;
import org.jgrapht.Graph;
import org.jgrapht.UndirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class OrganizationSingleDatasetResolution {


    public static void main(String arg[]) {


        // DATASETS
        DataSet<Record, Attribute> openaire = new HashedDataSet<>();
//        DataSet<Record, Attribute> cercauniversita = new HashedDataSet<>();
//        DataSet<Record, Attribute> arianna = new HashedDataSet<>();
//        DataSet<Record, Attribute> questio = new HashedDataSet<>();
//        DataSet<Record, Attribute> bvd = new HashedDataSet<>();
//        DataSet<Record, Attribute> cnr = new HashedDataSet<>();
//        DataSet<Record, Attribute> scanr = new HashedDataSet<>();
//        DataSet<Record, Attribute> orcid = new HashedDataSet<>();
//        DataSet<Record, Attribute> all = new HashedDataSet<>();

        // /Users/paolosottovia/Downloads/extracted_data_17_03_2018/

//        String basePath = "/Users/paolosottovia/Downloads/extracted_data_17_03_2018/";

        String basePath = arg[0];
//        String mode = arg[1];

        System.out.println("BASEPATH: " + basePath);
//        System.out.println("MODE: " + mode);


        File cercaUniversitaFile = new File(basePath + "organizations_CercaUniversita.csv");

        File openaireFile = new File(basePath + "organizations_OpenAire.csv");

        File questioFile = new File(basePath + "organizations_Questio.csv");

        File ariannaFile = new File(basePath + "organizations_Arianna - Anagrafe Nazionale delle Ricerche.csv");

        File dvbFile = new File(basePath + "organizations_Bureau van Dijk.csv");

        File cnrFile = new File(basePath + "organizations_Consiglio Nazionale delle Ricerche (CNR).csv");

        File scanrFile = new File(basePath + "organizations_ScanR.csv");

        File orcidFile = new File(basePath + "organizations_ORCID.csv");


        //read data from csv Files
        try {
//            new CSVRecordReader(-1).loadFromCSV(
//                    cercaUniversitaFile,
//                    cercauniversita);
            new CSVRecordReader(-1).loadFromCSV(
                    openaireFile, openaire);

//            new CSVRecordReader(-1).loadFromCSV(
//                    questioFile, questio);
//
//            new CSVRecordReader(-1).loadFromCSV(
//                    ariannaFile, arianna);
//
//            new CSVRecordReader(-1).loadFromCSV(
//                    dvbFile, bvd);
//
//            new CSVRecordReader(-1).loadFromCSV(
//                    cnrFile, cnr);
//
//            new CSVRecordReader(-1).loadFromCSV(
//                    scanrFile, scanr);
//
//            new CSVRecordReader(-1).loadFromCSV(
//                    orcidFile, orcid);

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Number of items in Openaire: " + openaire.size());
//        System.out.println("Number of items in cerca universita: " + cercauniversita.size());
//        System.out.println("Number of items in questio: " + questio.size());
//        System.out.println("Number of items in arianna: " + arianna.size());
//        System.out.println("Number of items in bvd: " + bvd.size());
//        System.out.println("Number of items in cnr: " + bvd.size());
//        System.out.println("Number of items in scanr: " + scanr.size());
//        System.out.println("Number of items in orcid: " + orcid.size());


        OrganizationSingleDatasetResolution OSDR = new OrganizationSingleDatasetResolution();

        OrganizationDBCleaner cleaner = new OrganizationDBCleaner();
        cleaner.cleanDB(openaire);


        System.out.println("DEBUG: ");

        Attribute LABEL = new Attribute("label");
        Attribute ADDRESS = new Attribute("address");
        Attribute CITY = new Attribute("city");
        Attribute LINKS = new Attribute("links");
        for (Record record : openaire.get()) {
            System.out.println(record.getValue(LABEL) + "\t" + record.getValue(ADDRESS) + "\t" + record.getValue(CITY) + "\t" + record.getValue(LINKS));
        }


        System.out.println("\nEND DEBUG");


        List<Set<String>> matches = OSDR.runIdentityResolution(openaire);


        System.out.println("DONE");


    }


    public List<Set<String>> runIdentityResolution(DataSet<Record, Attribute> dataset) {


        OrganizationDBCleaner cleaner = new OrganizationDBCleaner();
        cleaner.cleanDB(dataset);

//        Attribute labelOpenAire = new Attribute("label");
//        Attribute labelCercaUniversita = new Attribute("label");
        Attribute attributeId = new Attribute("id");


        LinearCombinationMatchingRule<Record, Attribute> matchingRule = new LinearCombinationMatchingRule<>(0.9);


        try {
//            RecordComparatorLabelLeveinstein lab = new RecordComparatorLabelLeveinstein(Organization.LABEL, Organization.LABEL);
//            lab.setLowerCase(true);
//            lab.setIgnoreNull(true);
//            matchingRule.addComparator(lab, 0.5d);
//
//
//            RecordComparatorLevenshtein city = new RecordComparatorLevenshtein(Organization.CITY, Organization.CITY);
//            city.setLowerCase(true);
//            city.setIgnoreNull(true);
//            matchingRule.addComparator(city, 0.25d);
//
//
//            UrlComparator url = new UrlComparator(Organization.LINK, Organization.LINK);
//            url.setLowerCase(true);
//            url.setIgnoreNull(true);
//            matchingRule.addComparator(url, 0.25d);




            RecordComparatorOrganizationOpenAire comparator = new RecordComparatorOrganizationOpenAire(Organization.LABEL, Organization.LABEL);
            matchingRule.addComparator(comparator, 1.0d);

        } catch (Exception e) {
            e.printStackTrace();
        }

        StandardRecordBlocker<Record, Attribute> blocker =
                new StandardRecordBlocker<>(new OrganizationBlockingLabelQGramsOpenAire());


        Graph<String, DefaultEdge> g
                = new SimpleGraph<>(DefaultEdge.class);


        MatchingEngine<Record, Attribute> engine = new MatchingEngine<>();
//                    Processable<Correspondence<Record, Attribute>> correspondences = engine.runIdentityResolution(openaire,
//                            bvd, null, matchingRule, blocker);

        Processable<Correspondence<Record, Attribute>> correspondences = engine.runIdentityResolution(dataset,
                dataset, null, matchingRule, blocker);


        System.out.println("Number of correspondences: " + correspondences.size());


        Collection<Correspondence<Record, Attribute>> corresp = correspondences.get();


        Collection<Correspondence<Record, Attribute>> correspFiltered = new ArrayList<>();
        for (Correspondence<Record, Attribute> corr : corresp) {

            Record r1 = corr.getFirstRecord();

            Record r2 = corr.getSecondRecord();
            if (!r1.getValue(attributeId).equals(r2.getValue(attributeId))) {
                correspFiltered.add(corr);
            }
        }


        System.out.println("Number of correspondences filtered: " + correspFiltered.size());


        Map<String, Set<Record>> ids = new HashMap<>();


        Set<Set<String>> connectedComponents = new HashSet<>();

        Map<String, Record> idRecords = new HashMap<>();
        Set<String> allIds = new HashSet<>();


        Collection<Record> records = dataset.get();
        for (Record r : records) {
            idRecords.put(r.getValue(attributeId), r);
        }


        printSingleCorrespondence(correspFiltered, idRecords);

        //Given all the correspondences create a graph
        //Nodes are the entities
        //Edges are the correspondences


        Map<Set<String>, Correspondence<Record, Attribute>> edgeCorrespondences = new HashMap<>();
        for (Correspondence<Record, Attribute> corr : correspFiltered) {

            Record r1 = corr.getFirstRecord();
            Record r2 = corr.getSecondRecord();

            String id1 = r1.getValue(attributeId);
            String id2 = r2.getValue(attributeId);

            g.addVertex(id1);
            g.addVertex(id2);
            g.addEdge(id1, id2);

            Set<String> edgeComponent = new HashSet<>();
            edgeComponent.add(id1);
            edgeComponent.add(id2);

            if (edgeCorrespondences.containsKey(edgeComponent)) {
                System.err.println("Error duplicate edge");
            } else {
                edgeCorrespondences.put(edgeComponent, corr);
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


        System.out.println("MATCH RESULTS: ");


        Collections.sort(connectedComponentsCleaned, new Comparator<Set<String>>() {
            @Override
            public int compare(Set<String> o1, Set<String> o2) {
                return -1 * Integer.compare(o1.size(), o2.size());
            }
        });

        printCorrespondences(connectedComponentsCleaned, idRecords);

        return connectedComponentsCleaned;
    }


    private void printSingleCorrespondence(Collection<Correspondence<Record, Attribute>> correspondences, Map<String, Record> idRecords) {


        Attribute LABEL = new Attribute("label");
        Attribute ADDRESS = new Attribute("address");
        Attribute CITY = new Attribute("city");
        Attribute LINKS = new Attribute("links");


        for (Correspondence<Record, Attribute> corr : correspondences) {

            System.out.println("\n\n\n===============================================================");
            System.out.println("SIMILARITY: " + corr.getSimilarityScore());
            System.out.println("LABEL\tADDRESS\tCITY\tLINKS");


            Collection<Record> records = new ArrayList<>();
            records.add(corr.getFirstRecord());
            records.add(corr.getSecondRecord());

            for (Record record : records) {


                System.out.println(record.getValue(LABEL) + "\t" + record.getValue(ADDRESS) + "\t" + record.getValue(CITY) + "\t" + record.getValue(LINKS));

            }


        }


    }

    private void printCorrespondences(List<Set<String>> connectedComponents, Map<String, Record> idRecords) {


        Attribute LABEL = new Attribute("label");
        Attribute ADDRESS = new Attribute("address");
        Attribute CITY = new Attribute("city");
        Attribute LINKS = new Attribute("links");


        for (Set<String> set : connectedComponents) {

            System.out.println("\n\n\n===============================================================");
            System.out.println("SIZE: " + set.size());
            System.out.println("LABEL\tADDRESS\tCITY\tLINKS");

            for (String id : set) {
                Record record = idRecords.get(id);
                System.out.println(record.getValue(LABEL) + "\t" + record.getValue(ADDRESS) + "\t" + record.getValue(CITY) + "\t" + record.getValue(LINKS));

            }


        }


//        System.out.println("TEST: ");
//        String id1 = "21401";
//        String id2 = "23703";
//        Record record1 = idRecords.get(id1);
//        Record record2 = idRecords.get(id2);
//
//        System.out.println(record1.getValue(LABEL) + "\t" + record1.getValue(ADDRESS) + "\t" + record1.getValue(CITY) + "\t" + record1.getValue(LINKS));
//        System.out.println(record2.getValue(LABEL) + "\t" + record2.getValue(ADDRESS) + "\t" + record2.getValue(CITY) + "\t" + record2.getValue(LINKS));


    }


}
