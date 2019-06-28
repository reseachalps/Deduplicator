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
import it.unimore.comparators.RecordComparator;
import it.unimore.comparators.RecordComparatorLabelJaccard3Gram;
import it.unimore.comparators.RecordComparatorLabelLeveinstein;
import it.unimore.comparators.RecordComparatorOrganizationOpenAire;
import it.unimore.deduplication.model.Organization;
import org.jgrapht.Graph;
import org.jgrapht.alg.BiconnectivityInspector;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.alg.clique.BronKerboschCliqueFinder;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

import java.io.*;
import java.util.*;

public class OrganizationORCIDDatasetResolution {


    public static void main(String arg[]) {


        // DATASETS

        DataSet<Record, Attribute> orcidParents = new HashedDataSet<>();
        String basePath = arg[0];
        String basePathFull = arg[1];
//        String mode = arg[1];

        System.out.println("BASEPATH: " + basePath);

        File orcidFileParent = new File(basePath + "organizations_ORCID_parent.csv");


        //read data from csv Files
        try {

            new CSVRecordReader(-1).loadFromCSV(
                    orcidFileParent, orcidParents);

        } catch (IOException e) {
            e.printStackTrace();
        }


        DataSet<Record, Attribute> orcid = new HashedDataSet<>();

        File orcidFile = new File(basePathFull + "organizations_ORCID.csv");


        //read data from csv Files
        try {

            new CSVRecordReader(-1).loadFromCSV(
                    orcidFile, orcid);

        } catch (IOException e) {
            e.printStackTrace();
        }

        OrganizationORCIDDatasetResolution orcidResolutor = new OrganizationORCIDDatasetResolution();

        List<Set<String>> allMatches = orcidResolutor.retrieveOrcidMatches(orcidParents, orcid);


        System.out.println("Final MATCHES");

        System.out.println("All matches: " + allMatches.size());

        Collections.sort(allMatches, new Comparator<Set<String>>() {
            @Override
            public int compare(Set<String> o1, Set<String> o2) {
                return -1 * Integer.compare(o1.size(), o2.size());
            }
        });


        Map<String, Record> idRecord = new HashMap<>();


        for (Record r : orcid.get()) {
            idRecord.put(r.getValue(Organization.ID), r);
        }


        for (Set<String> match : allMatches) {

            System.out.println("================SIZE: " + match.size() + "=================");

            for (String id : match) {
                Record record = idRecord.get(id);
                System.out.println(id + "\t" + record.getValue(Organization.LABEL) + "\t" + record.getValue(Organization.ADDRESS) + "\t" + record.getValue(Organization.CITY) + "\t" + record.getValue(Organization.LINK));
            }


        }


    }


    public List<Set<String>> retrieveOrcidMatches(DataSet<Record, Attribute> orcidParents, DataSet<Record, Attribute> orcid) {

        Attribute LABEL = new Attribute("label");
        Attribute ADDRESS = new Attribute("address");
        Attribute CITY = new Attribute("city");
        Attribute LINKS = new Attribute("links");
        Attribute ID = new Attribute("id");
        Attribute CHILDREN = new Attribute("children");
        Map<String, Record> idRecord = new HashMap<>();


        System.out.println("INPUT DATA: ");

        for (Record record : orcidParents.get()) {

            String id = record.getValue(ID);

            idRecord.put(id, record);

            System.out.println(id + "\t" + record.getValue(LABEL) + "\t" + record.getValue(ADDRESS) + "\t" + record.getValue(CITY) + "\t" + record.getValue(LINKS));
        }


        System.out.println("Number of items in orcid: " + orcidParents.size());


        OrganizationORCIDDatasetResolution OSDR = new OrganizationORCIDDatasetResolution();

        OrganizationDBCleaner cleaner = new OrganizationDBCleaner();
        cleaner.cleanDB(orcidParents);


        System.out.println("DEBUG AFTER CLEANING   : ");

        for (Record record : orcidParents.get()) {

            String id = record.getValue(ID);

            idRecord.put(id, record);

            System.out.println(id + "\t" + record.getValue(LABEL) + "\t" + record.getValue(ADDRESS) + "\t" + record.getValue(CITY) + "\t" + record.getValue(LINKS));
        }


        System.out.println("\nEND DEBUG");


        List<Set<String>> matchesParent = OSDR.runIdentityResolution(orcidParents);


        Map<Set<String>, Set<String>> matchesChildren = new HashMap<>();


        Set<String> entitiesJoined = new HashSet<>();
        for (Set<String> match : matchesParent) {

            Set<String> children = new HashSet<>();
            for (String id : match) {

                children.addAll(getChildrenIds(idRecord.get(id), CHILDREN));


            }


            matchesChildren.put(match, children);
            entitiesJoined.addAll(match);


        }

        int count = 0;
        Map<String, Integer> idGroup = new HashMap<>();

        for (Map.Entry<Set<String>, Set<String>> entry : matchesChildren.entrySet()) {

            for (String id : entry.getValue()) {
                idGroup.put(id, count);
            }

            count++;
        }

        for (Record r : orcidParents.get()) {

            String id = r.getValue(ID);
            if (!entitiesJoined.contains(id)) {

                for (String ch : getChildrenIds(r, CHILDREN)) {
                    idGroup.put(ch, count);

                }

                count++;
            }

        }


        System.out.println("Number of groups: " + count);

        System.out.println("Number of entries: " + idGroup.size());

        //


        Set<String> idParents = idRecord.keySet();


        DataSet<Record, Attribute> orcidChildren = new HashedDataSet<>();


        for (Record record : orcid.get()) {

            String id = record.getValue(ID);
            if (!idParents.contains(id)) {
                orcidChildren.add(record);
            }

        }


        System.out.println("CHILDREN!!!!!!!");

        List<Set<String>> childrenMatches = OSDR.runIdentityResolutionChildren(orcidChildren, idGroup);


        System.out.println("DONE");

        List<Set<String>> allMatches = new ArrayList<>();
        allMatches.addAll(matchesParent);
        allMatches.addAll(childrenMatches);

        return allMatches;
    }


    private static List<String> getChildrenIds(Record record, Attribute CHILDREN) {

        String children = record.getValue(CHILDREN);

        List<String> childs = new ArrayList<>();


        if (!children.trim().equals("")) {

//            System.out.println("INPUT: " + children);

            if (!children.trim().contains(";")) {
                String id = children.substring(0, children.indexOf('_'));
//                System.out.println("\tOriginal: " + children.trim() + "\tmod: " + id);
                childs.add(id);
            } else {
                String cs[] = children.trim().split(";", -1);
                for (String c : cs) {

//                    System.out.println("INPUT: " + c);

                    if (c.contains("_")) {
                        String id = c.substring(0, c.indexOf('_'));
//                        System.out.println("\tOriginal: " + c.trim() + "\tmod: " + id);
                        childs.add(id);
                    }
                }


            }


        }


        return childs;
    }


    public List<Set<String>> runIdentityResolutionChildren(DataSet<Record, Attribute> dataset, Map<String, Integer> idGroup) {


        System.out.println("runIdentityResolutionChildren START");
        OrganizationDBCleaner cleaner = new OrganizationDBCleaner();
        cleaner.cleanDB(dataset);

        Attribute attributeId = new Attribute("id");


        LinearCombinationMatchingRule<Record, Attribute> matchingRule = new LinearCombinationMatchingRule<>(0.9);


        try {
            RecordComparatorLabelLeveinstein lab = new RecordComparatorLabelLeveinstein(Organization.LABEL, Organization.LABEL);
            lab.setLowerCase(true);
            lab.setIgnoreNull(false);
            matchingRule.addComparator(lab, 0.5d);


            RecordComparatorLevenshtein city = new RecordComparatorLevenshtein(Organization.CITY, Organization.CITY);
            city.setLowerCase(true);
            city.setIgnoreNull(true);
            matchingRule.addComparator(city, 0.5d);

        } catch (Exception e) {
            e.printStackTrace();
        }

        StandardRecordBlocker<Record, Attribute> blocker =
                new StandardRecordBlocker<>(new OrganizationBlockingCountryLabelQGramsChildren(idGroup));


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

            System.out.println("");

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


        Map<String, String> id_Label = new HashMap<>();
        Attribute LABEL = new Attribute("label");

        Map<Set<String>, Correspondence<Record, Attribute>> edgeCorrespondences = new HashMap<>();
        for (Correspondence<Record, Attribute> corr : correspFiltered) {

            Record r1 = corr.getFirstRecord();
            Record r2 = corr.getSecondRecord();

            String id1 = r1.getValue(attributeId);
            String id2 = r2.getValue(attributeId);

            id_Label.put(id1, r1.getValue(LABEL));
            id_Label.put(id2, r2.getValue(LABEL));

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
        ConnectivityInspector<String, DefaultEdge> c = new ConnectivityInspector<String, DefaultEdge>((Graph<String, DefaultEdge>) g);


//        List<Set<String>> connectedComponentsLIST = new ArrayList<>();
//        connectedComponentsLIST.addAll(c.connectedSets());

//        printTopKConnectedComponents(g, connectedComponentsLIST, id_Label, 15);

        List<Set<String>> connectedComponentsCleaned = new ArrayList<>();

        if (g.edgeSet().size() > 0) {

            BiconnectivityInspector<String, DefaultEdge> c1 = new BiconnectivityInspector<>(g);
            BronKerboschCliqueFinder<String, DefaultEdge> c2 = new BronKerboschCliqueFinder<>((Graph<String, DefaultEdge>) g);

            connectedComponentsCleaned.addAll(c.connectedSets());
        }

        System.out.println("Number of connected components: " + connectedComponentsCleaned.size());


        System.out.println("MATCH RESULTS: ");


        printCorrespondences(connectedComponentsCleaned, idRecords);

        return connectedComponentsCleaned;
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
////            lab.setLowerCase(true);
////            lab.setIgnoreNull(false);
////            matchingRule.addComparator(lab, 0.6d);
////

            RecordComparatorLabelJaccard3Gram gramsComparator = new RecordComparatorLabelJaccard3Gram(Organization.LABEL, Organization.LABEL);
            matchingRule.addComparator(gramsComparator, 0.6d);


            RecordComparatorLevenshtein city = new RecordComparatorLevenshtein(Organization.CITY, Organization.CITY);
            city.setLowerCase(true);
            city.setIgnoreNull(true);
            city.setNullReturn(0.9d);
            matchingRule.addComparator(city, 0.4d);


        } catch (Exception e) {
            e.printStackTrace();
        }

        StandardRecordBlocker<Record, Attribute> blocker =
                new StandardRecordBlocker<>(new OrganizationBlockingCountryCityQGrams());


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

            System.out.println(r1.getValue(Organization.LABEL) + "\t" + r1.getValue(Organization.ADDRESS) + "\t" + r1.getValue(Organization.CITY) + "\t" + r1.getValue(Organization.LINK));
            System.out.println(r2.getValue(Organization.LABEL) + "\t" + r2.getValue(Organization.ADDRESS) + "\t" + r2.getValue(Organization.CITY) + "\t" + r2.getValue(Organization.LINK));
            System.out.println("\n\n");
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


        Map<String, String> id_Label = new HashMap<>();
        Attribute LABEL = new Attribute("label");

        Map<Set<String>, Correspondence<Record, Attribute>> edgeCorrespondences = new HashMap<>();
        for (Correspondence<Record, Attribute> corr : correspFiltered) {

            Record r1 = corr.getFirstRecord();
            Record r2 = corr.getSecondRecord();

            String id1 = r1.getValue(attributeId);
            String id2 = r2.getValue(attributeId);

            id_Label.put(id1, r1.getValue(LABEL));
            id_Label.put(id2, r2.getValue(LABEL));

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
        ConnectivityInspector<String, DefaultEdge> c = new ConnectivityInspector<String, DefaultEdge>((Graph<String, DefaultEdge>) g);


//        List<Set<String>> connectedComponentsLIST = new ArrayList<>();
//        connectedComponentsLIST.addAll(c.connectedSets());

//        printTopKConnectedComponents(g, connectedComponentsLIST, id_Label, 15);


        List<Set<String>> connectedComponentList = c.connectedSets();

        int numberOfComponent = 0;
        int count = 0;

        for (Set<String> vertexes : connectedComponentList) {

            Graph<String, DefaultEdge> subgraph = new SimpleGraph<>(DefaultEdge.class);
            Set<DefaultEdge> subGraphEdges = new HashSet<>();
            for (String vertex : vertexes) {
                Set<DefaultEdge> edges = g.edgesOf(vertex);
                subGraphEdges.addAll(edges);
                subgraph.addVertex(vertex);
            }

            for (DefaultEdge edge : subGraphEdges) {
                String source = g.getEdgeSource(edge);
                String destination = g.getEdgeTarget(edge);
                subgraph.addEdge(source, destination);
            }
            System.out.println("============================================================================================================================");

            System.out.println("-----------------------------------------------------------");

            System.out.println("Vertexes: " + vertexes.size());
            for (String vertex : vertexes) {
                Record record = idRecords.get(vertex);
                System.out.println(vertex + "\t" + record.getValue(Organization.LABEL) + "\t" + record.getValue(Organization.ADDRESS) + "\t" + record.getValue(Organization.CITY) + "\t" + record.getValue(Organization.LINK));
            }


            BiconnectivityInspector<String, DefaultEdge> biconnectivityInspectorSub = new BiconnectivityInspector<>((Graph<String, DefaultEdge>) subgraph);

            Set<Set<String>> biconnectedComponents = biconnectivityInspectorSub.getBiconnectedVertexComponents();

            System.out.println("Number of binconnected components: " + biconnectedComponents.size());

            for (Set<String> eee : biconnectedComponents) {

                System.out.println("\t-----------------------------------");
                for (String v : eee) {
                    Record record = idRecords.get(v);
                    System.out.println(v + "\t" + record.getValue(Organization.LABEL) + "\t" + record.getValue(Organization.ADDRESS) + "\t" + record.getValue(Organization.CITY) + "\t" + record.getValue(Organization.LINK));


                }
            }
            BronKerboschCliqueFinder<String, DefaultEdge> bronKerboschCliqueFinder = new BronKerboschCliqueFinder<>((Graph<String, DefaultEdge>) subgraph);

            List<Set<String>> maximalCliques = new ArrayList<>();

            Iterator<Set<String>> it = bronKerboschCliqueFinder.iterator();


            if (count < 300) {

                // printGraph("subgraphs/subgraph", numberOfComponent, subgraph, subGraphEdges, idRecords);
                count++;

            }
            while (it.hasNext()) {
                Set<String> clique = it.next();
                maximalCliques.add(clique);
            }

            System.out.println("MAXIMAL CLIQUES");

            for (Set<String> eee : maximalCliques) {

                System.out.println("\t\t-----------------------------------");
                for (String v : eee) {
                    Record record = idRecords.get(v);
                    System.out.println("\t\t" + v + "\t" + record.getValue(Organization.LABEL) + "\t" + record.getValue(Organization.ADDRESS) + "\t" + record.getValue(Organization.CITY) + "\t" + record.getValue(Organization.LINK));


                }
            }

            numberOfComponent++;


        }


//        BiconnectivityInspector<String, DefaultEdge> c1 = new BiconnectivityInspector<>(g);
//
//
//        BronKerboschCliqueFinder<String, DefaultEdge> c2 = new BronKerboschCliqueFinder<>((Graph<String, DefaultEdge>) g);
//
////        Set<Set<String>> biconnectedComponentsCleanedSet = c1.getBiconnectedVertexComponents();
//
//        List<Set<String>> maximalCliques = new ArrayList<>();
//
//        Iterator<Set<String>> it = c2.iterator();
//
//        int countCliques = 0;
//
//        while (it.hasNext()) {
//            Set<String> clique = it.next();
//            maximalCliques.add(clique);
//            countCliques++;
//        }
//
//        System.out.println("Cliques");
//        List<Set<String>> connectedComponentsLIST = new ArrayList<>();
//        connectedComponentsLIST.addAll(maximalCliques);
//        int k = 15;
//        if (connectedComponentsLIST.size() < k) {
//            k = connectedComponentsLIST.size();
//        }
//
//        printTopKConnectedComponents(g, connectedComponentsLIST, idRecords, k);
//
//
//        System.out.println("Number of binconnected components: " + c1.getBiconnectedVertexComponents().size());
//
//
//        System.out.println("Number of maximal clique components: " + maximalCliques.size());


        //Connected components represent the set of entities that represent the same real-wold entity
//        List<Set<String>> connectedComponentsCleaned = c.connectedSets();

        List<Set<String>> connectedComponentsCleaned = new ArrayList<>();
        connectedComponentsCleaned.addAll(connectedComponentList);

        System.out.println("Number of connected components: " + connectedComponentsCleaned.size());


        System.out.println("MATCH RESULTS: ");


        printCorrespondences(connectedComponentsCleaned, idRecords);

        return connectedComponentsCleaned;
    }


    private void printTopKConnectedComponents(Graph<String, DefaultEdge> graph, List<Set<String>> connectedComponentsLIST, Map<String, Record> idRecord, int k) {


        Collections.sort(connectedComponentsLIST, new Comparator<Set<String>>() {
            @Override
            public int compare(Set<String> o1, Set<String> o2) {
                return -1 * Integer.compare(o1.size(), o2.size());
            }
        });


        List<Set<String>> connectedComponentsLISTFiltered = connectedComponentsLIST.subList(0, k);


//        Set<DefaultEdge> edges=graph.incomingEdgesOf();

        int i = 0;
        for (Set<String> component : connectedComponentsLISTFiltered) {

            Set<DefaultEdge> edges = new HashSet<>();

            for (String vertex : component) {

                edges.addAll(graph.edgesOf(vertex));


            }

            System.out.println("Number elements in the components: " + component.size());
            System.out.println("Number of edges: " + edges.size());


            printGraph("graphs/sample", i, graph, edges, idRecord);
            i++;

        }


    }


    public static void printGraph(String basepath, int filenumber, Graph<String, DefaultEdge> graph, Set<DefaultEdge> edges, Map<String, Record> idRecord) {


        BufferedWriter writer = null;

//        System.out.println("Number of edges: " + edges.size());
        try {
            writer = new BufferedWriter(new FileWriter(basepath + "_" + filenumber + ".dot"));
            writer.write("graph {\n");

            for (DefaultEdge edge : edges) {

                String left = "_" + graph.getEdgeSource(edge) + "_" + cleanVertex(idRecord.get(graph.getEdgeSource(edge)).getValue(Organization.LABEL)) + "_" + cleanVertex(idRecord.get(graph.getEdgeSource(edge)).getValue(Organization.CITY));
                String right = "_" + graph.getEdgeTarget(edge) + "_" + cleanVertex(idRecord.get(graph.getEdgeTarget(edge)).getValue(Organization.LABEL)) + "_" + cleanVertex(idRecord.get(graph.getEdgeSource(edge)).getValue(Organization.CITY));

                writer.write(left + " -- " + right + "\n");


            }
            writer.write("}");

            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
//        writer.write(str);


    }

    private static String cleanVertex(String vertex) {
        if (vertex != null) {
            return "_" + vertex.replace(" ", "_").toLowerCase().replace("-", "").replace(".", "").
                    replace("'", "").replace("`", "").replace("/", "_").replace("&", "_").
                    replace("(", "_").replace(")", "_").replace("|", "_").replace("\\|", "_") + "_";
        } else {
            return "NULL";
        }
    }


    private void printSingleCorrespondence(Collection<Correspondence<Record, Attribute>> correspondences, Map<String, Record> idRecords) {

        Attribute ID = new Attribute("id");
        Attribute LABEL = new Attribute("label");
        Attribute ADDRESS = new Attribute("address");
        Attribute CITY = new Attribute("city");
        Attribute LINKS = new Attribute("links");


        for (Correspondence<Record, Attribute> corr : correspondences) {

            System.out.println("\n\n\n===============================================================");
            System.out.println("SIMILARITY: " + corr.getSimilarityScore());
            System.out.println("ID\tLABEL\tADDRESS\tCITY\tLINKS");


            Collection<Record> records = new ArrayList<>();
            records.add(corr.getFirstRecord());
            records.add(corr.getSecondRecord());

            for (Record record : records) {


                System.out.println(record.getValue(ID) + "\t" + record.getValue(LABEL) + "\t" + record.getValue(ADDRESS) + "\t" + record.getValue(CITY) + "\t" + record.getValue(LINKS));

            }


        }


    }

    private void printCorrespondences(List<Set<String>> connectedComponents, Map<String, Record> idRecords) {


        Attribute LABEL = new Attribute("label");
        Attribute ADDRESS = new Attribute("address");
        Attribute CITY = new Attribute("city");
        Attribute LINKS = new Attribute("links");


        String fileName = "out_orcid_matches.log";

        FileWriter fileWriter = null;
        PrintWriter printWriter = null;
        try {
            fileWriter = new FileWriter(fileName);
            printWriter = new PrintWriter(fileWriter);
        } catch (IOException e) {
            e.printStackTrace();
        }


        for (Set<String> set : connectedComponents) {

            System.out.println("\n\n\n===============================================================");
            System.out.println("SIZE: " + set.size());
            printWriter.println("\n\n\n===============================================================");
            System.out.println("LABEL\tADDRESS\tCITY\tLINKS");
            printWriter.println("SIZE: " + set.size());
            printWriter.println("LABEL\tADDRESS\tCITY\tLINKS");
            System.out.println("SIZE: " + set.size());

            for (String id : set) {
                Record record = idRecords.get(id);
                String logMessage = id + "\t" + record.getValue(LABEL) + "\t" + record.getValue(ADDRESS) + "\t" + record.getValue(CITY) + "\t" + record.getValue(LINKS);
                System.out.println(logMessage);
                printWriter.println(logMessage);
            }
        }

        printWriter.close();


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
