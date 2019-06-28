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
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import it.unimore.comparators.RecordComparatorOrganization;
import it.unimore.deduplication.*;
import it.unimore.deduplication.model.Organization;
import org.apache.jena.base.Sys;
import org.apache.jena.query.Dataset;
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


public class OrganizationDeduplicationExperimentsScalability {


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
        DataSet<Record, Attribute> p3 = new HashedDataSet<>();
        DataSet<Record, Attribute> grid = new HashedDataSet<>();
        DataSet<Record, Attribute> fwf = new HashedDataSet<>();
        DataSet<Record, Attribute> sicris = new HashedDataSet<>();
        DataSet<Record, Attribute> foen = new HashedDataSet<>();
        // /Users/paolosottovia/Downloads/extracted_data_17_03_2018/

//        String basePath = "/Users/paolosottovia/Downloads/extracted_data_17_03_2018/";

        String basePath = arg[0];
        String mode = arg[1];
        String test = arg[2];

        System.out.println("BASEPATH: " + basePath);
        System.out.println("MODE: " + mode);

        boolean includeOrcid = true;


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

        File foenFile = new File(basePath + "organizations_Federal Office for the Environment - FOEN");
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

            new CSVRecordReader(-1).loadFromCSV(
                    foenFile, foen);

        } catch (IOException e) {
            e.printStackTrace();
        }


        List<DataSet> listOfDataset = new ArrayList<>();


        System.out.println("\nNumber of items in Openaire: " + openaire.size());
        System.out.println("Number of items in cerca universita: " + cercauniversita.size());
        System.out.println("Number of items in questio: " + questio.size());
        System.out.println("Number of items in arianna: " + arianna.size());
        System.out.println("Number of items in bvd: " + bvd.size());
        System.out.println("Number of items in cnr: " + bvd.size());
        System.out.println("Number of items in scanr: " + scanr.size());
        System.out.println("Number of items in registro imprese: " + registroImprese.size());
        System.out.println("Number of items in patiris: " + patiris.size());
        System.out.println("Number of items in p3: " + p3.size());
        System.out.println("Number of items in grid: " + grid.size());
        System.out.println("Number of items in fwf: " + fwf.size());

        System.out.println("Number of items in sicris: " + sicris.size());
        System.out.println("Number of items in foen: " + foen.size());

        if (includeOrcid) {
            System.out.println("Number of items in orcid: " + orcid.size());

            System.out.println("Number of items in orcid parent: " + orcidParent.size());
        }
        List<List<DataSet<Record, Attribute>>> ALL_DATASETS = new ArrayList<>();


        if (TEST) {
            System.out.println("===============================================================");

            for (DataSet<Record, Attribute> dataset : listOfDataset) {
                for (Record record : dataset.get()) {
                    System.out.println("\t" + record.getValue(Organization.ID) + "\t" + record.getValue(Organization.LABEL) + "\t" + record.getValue(Organization.ADDRESS) + "\t" + record.getValue(Organization.CITY) + "\t" + record.getValue(Organization.LINK));


                }
            }


            System.out.println("===============================================================");
        }


        //Cleaner
        OrganizationDBCleaner cleaner = new OrganizationDBCleaner();
        cleaner.cleanDB(openaire);
        cleaner.cleanDB(cercauniversita);
        cleaner.cleanDB(questio);
        cleaner.cleanDB(arianna);
        cleaner.cleanDB(bvd);
        cleaner.cleanDB(cnr);
        cleaner.cleanDB(openaire);
        cleaner.cleanDB(scanr);
        cleaner.cleanDB(registroImprese);
        cleaner.cleanDB(patiris);

        cleaner.cleanDB(fwf);
        cleaner.cleanDB(sicris);

        cleaner.cleanDB(p3);
        cleaner.cleanDB(grid);
        cleaner.cleanDB(foen);
        if (includeOrcid) {
            cleaner.cleanDB(orcid);
            cleaner.cleanDB(orcidParent);
        }

        //ORCID
        System.out.println("\n\n\n\n==============START DEDUPLICATION OF ORCID=================\n");

        OrganizationORCIDDatasetResolution OSDROrcid = new OrganizationORCIDDatasetResolution();

        Map<String, Set<String>> candidateOthersORCID = new HashMap<>();

        Set<String> orcidIdToMaintain = new HashSet<>();
        Set<String> orcidIdToRemove = new HashSet<>();
        List<Set<String>> matchesOrcid = new ArrayList<>();
        if (includeOrcid) {
            matchesOrcid = OSDROrcid.retrieveOrcidMatches(orcidParent, orcid);
            System.out.println("Matches ORCID");
            for (Set<String> match : matchesOrcid) {
                System.out.println("" + match.toString());
            }

            Map<String, Record> idRecordORCID = new HashMap<>();
            for (Record r : orcid.get()) {
                idRecordORCID.put(r.getValue(Organization.ID), r);
            }


            for (Set<String> matchOrcid : matchesOrcid) {
                List<String> items = new ArrayList<>();
                items.addAll(matchOrcid);

                String candidate = items.get(0);
                orcidIdToMaintain.add(candidate);
                Set<String> others = new HashSet<>();
                for (int i = 1; i < items.size(); i++) {
                    orcidIdToRemove.add(items.get(i));
                    others.add(items.get(i));
                }
                candidateOthersORCID.put(candidate, others);


            }
        }

        System.out.println("Number of matches ORCID: " + matchesOrcid.size());
        System.out.println("==============END DEDUPLICATION OF ORCID=================\n\n\n\n");

        //OPENAIRE

        System.out.println("\n\n\n\n==============START DEDUPLICATION OF OPENAIRE=================\n");

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


                    int n1 = getNullValues(r1);
                    int n2 = getNullValues(r2);

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


        DataSet<Record, Attribute> openAireFiltered = filterDatasetByIds(openaire, openaireIdToRemove);


        System.out.println("OPENAIRE FILTERED:");

        for (Record record : openAireFiltered.get()) {

            System.out.println("\t" + record.getValue(Organization.ID) + "\t" + record.getValue(Organization.LABEL) + "\t" + record.getValue(Organization.ADDRESS) + "\t" + record.getValue(Organization.CITY) + "\t" + record.getValue(Organization.LINK));


        }
        System.out.println("END OPENAIRE!!!");
        System.out.println("Number of matches openaire: " + matchesOpenAire.size());
        System.out.println("==============END DEDUPLICATION OF OPENAIRE=================\n\n\n\n");


        List<DataSet<Record, Attribute>> dataSets = new ArrayList<>();
        dataSets.add(openaire);
        dataSets.add(cercauniversita);
        dataSets.add(questio);
        dataSets.add(arianna);
        dataSets.add(bvd);
        dataSets.add(cnr);
        dataSets.add(registroImprese);
        dataSets.add(patiris);
        if (includeOrcid) {
            dataSets.add(orcid);
        }
        dataSets.add(scanr);
        dataSets.add(p3);
        dataSets.add(grid);
        dataSets.add(fwf);
        dataSets.add(sicris);
        dataSets.add(foen);

        List<String> dataSetsName = new ArrayList<>();
        dataSetsName.add("openaire");
        dataSetsName.add("cercauniversita");
        dataSetsName.add("questio");
        dataSetsName.add("arianna");
        dataSetsName.add("bvd");
        dataSetsName.add("cnr");
        dataSetsName.add("registroImprese");
        dataSetsName.add("patiris");
        if (includeOrcid) {
            dataSetsName.add("orcid");
        }
        dataSetsName.add("scanr");
        dataSetsName.add("p3");
        dataSetsName.add("grid");
        dataSetsName.add("fwf");
        dataSetsName.add("sicris");
        dataSetsName.add("foen");

        Map<String, String> idSources = new HashMap<>();

        for (int i = 0; i < dataSets.size(); i++) {

            DataSet<Record, Attribute> data = dataSets.get(i);
            String datasetName = dataSetsName.get(i);

            for (Record record : data.get()) {

                String id = record.getValue(Organization.ID);

                idSources.put(id, datasetName);


            }

        }


        Map<String, Record> idRecords = new HashMap<>();
        Attribute attributeId = new Attribute("id");


        Graph<String, DefaultEdge> g
                = new SimpleGraph<>(DefaultEdge.class);


        Set<String> idsOpenAire = new HashSet<>();
        for (Set<String> matches : matchesOpenAire) {
            idsOpenAire.addAll(matches);
        }

        for (Record rOA : openaire.get()) {
            if (idsOpenAire.contains(rOA.getValue(attributeId))) {
                idRecords.put(rOA.getValue(attributeId), rOA);
            }
        }

        if (includeOrcid) {
            for (Record rOrcid : orcid.get()) {
                idRecords.put(rOrcid.getValue(Organization.ID), rOrcid);
            }
        }


        System.out.println("MATCH OPENAIRE");
        for (Set<String> match : matchesOpenAire) {


            System.out.println("===================================================================================================");
            System.out.println("" + match.toString());
            for (String id : match) {
                Record record = idRecords.get(id);
                System.out.println(id + "\t" + record.getValue(Organization.LABEL) + "\t" + record.getValue(Organization.ADDRESS) + "\t" + record.getValue(Organization.CITY) + "\t" + record.getValue(Organization.LINK) + "\t" + idSources.get(id));

            }


        }

        System.out.println("END MATCH OPENAIRE");


        if (includeOrcid) {


            System.out.println("MATCH ORCID");
            for (Set<String> match : matchesOrcid) {


                System.out.println("===================================================================================================");
                System.out.println("" + match.toString());
                for (String id : match) {
                    Record record = idRecords.get(id);
                    System.out.println(id + "\t" + record.getValue(Organization.LABEL) + "\t" + record.getValue(Organization.ADDRESS) + "\t" + record.getValue(Organization.CITY) + "\t" + record.getValue(Organization.LINK) + "\t" + idSources.get(id));

                }


            }

            System.out.println("END MATCH ORCID");


        }


        // REMOVE ORCID EXTRA!!!!

        System.out.println("GRAPH STATS: ");
        System.out.println("#Vertexes: " + g.vertexSet().size());
        System.out.println("#Edges: " + g.edgeSet().size());

        for (String v : orcidIdToMaintain) {
            g.addVertex(v);
        }


        for (String v : orcidIdToRemove) {
            g.removeVertex(v);
        }

        for (String v : openaireIdToRemove) {
            g.removeVertex(v);
        }

        System.out.println("GRAPH STATS: ");
        System.out.println("#Vertexes: " + g.vertexSet().size());
        System.out.println("#Edges: " + g.edgeSet().size());


        //join candidateOthers


        Map<String, Set<String>> idOtherCandidates = new HashMap<>();
        idOtherCandidates.putAll(candidateOthersOpenAire);
        idOtherCandidates.putAll(candidateOthersORCID);


        if (includeOrcid) {
            Set<String> idsORCID = new HashSet<>();
            for (Set<String> matches : matchesOrcid) {
                idsORCID.addAll(matches);
            }

            for (Record rO : orcid.get()) {
                if (idsORCID.contains(rO.getValue(attributeId))) {
                    idRecords.put(rO.getValue(attributeId), rO);
                }
            }
        }


        int number_of_datates[] = {2, 4, 8,15};

        Map<Integer, Long> n_execTimeResolution = new HashMap<>();
        Map<Integer, Long> n_execTimeClustering = new HashMap<>();


        for (int n_exec : number_of_datates) {


            for (int i = 0; i < 1; i++) {


                List<DataSet<Record, Attribute>> dataSetsFiltered = new ArrayList<>();
                Collections.shuffle(dataSets);
                dataSetsFiltered.addAll(dataSets.subList(0, n_exec));
                System.out.println("NUMBER OF DATASETs: " + dataSetsFiltered.size());

                System.out.println("RUN STANDARD ================================================================");
                runStandardEntityMatching(dataSetsFiltered, mode, idRecords, idSources, idOtherCandidates, includeOrcid, g, TEST, n_execTimeResolution, n_execTimeClustering,n_exec);
                System.out.println("RUN STANDARD ================================================================");
            }

        }



        for(int i = 0; i < number_of_datates.length;i++){
            System.out.println("Number of datasets: "+ number_of_datates[i]);
            System.out.println("Time blocking and pairwise matching: "+ n_execTimeResolution.get(number_of_datates[i]));
            System.out.println("Time clustering: "+ n_execTimeClustering.get(number_of_datates[i]));

            System.out.println("========================================\n");
        }


//        System.out.println("RUN STANDARD ================================================================");
//        runStandardEntityMatching(dataSets,mode,idRecords,idSources,idOtherCandidates,includeOrcid,g,TEST);
//        System.out.println("RUN STANDARD ================================================================");
//
//
//        System.out.println("RUN DISTANCE BASED ================================================================");
//        runDistanceBasedEntityMatching(dataSets,mode,idRecords,idSources,idOtherCandidates,includeOrcid,g,"DISTANCE",TEST);
//        System.out.println("RUN DISTANCE BASED ================================================================");


//        System.out.println("RUN STANDARD WITHOUT CLUSTERING ================================================================");
//        runStandardEntityMatchingNOCLUSTERING(dataSets,mode,idRecords,idSources,idOtherCandidates,includeOrcid,g,"NO_CLUSTERING",TEST);
//        System.out.println("RUN STANDARD WITHOUT CLUSTERING ================================================================");
//

    }

    public static void runStandardEntityMatching(List<DataSet<Record, Attribute>> dataSets, String mode, Map<String, Record> idRecords, Map<String, String> idSources, Map<String, Set<String>> idOtherCandidates, boolean includeOrcid, Graph<String, DefaultEdge> gSTART, boolean TEST, Map<Integer, Long> n_execTimeResolution,
                                                 Map<Integer, Long> n_execTimeClustering,int n_exec) {

        // HERE


        long start = System.currentTimeMillis();

        Graph<String, DefaultEdge> g
                = new SimpleGraph<>(DefaultEdge.class);


//        EdgeFactory<String,DefaultEdge> factory = new

        for (DefaultEdge ed : gSTART.edgeSet()) {
            String left = gSTART.getEdgeSource(ed);
            String right = gSTART.getEdgeTarget(ed);
            g.addEdge(left, right);

        }

        LinearCombinationMatchingRule<Record, Attribute> matchingRule = new LinearCombinationMatchingRule<>(0.85);


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


        //


        try {


            RecordComparatorLevenshtein city = new RecordComparatorLevenshtein(Organization.CITY, Organization.CITY);
            city.setLowerCase(true);

            RecordComparatorLevenshtein link = new RecordComparatorLevenshtein(Organization.LINK, Organization.LINK);
            link.setLowerCase(true);

            RecordComparatorOrganization organizationComparator = new RecordComparatorOrganization(Organization.LABEL, Organization.LABEL);
            if (TEST) {
                organizationComparator.setDebug(true);
            }
            matchingRule.addComparator(organizationComparator, 1.0d);


            // Initialize Matching Engine
            Collection<Correspondence<Record, Attribute>> allCorrespondances = new ArrayList<>();


            Attribute LABEL = new Attribute("label");
            Attribute ADDRESS = new Attribute("address");
            Attribute CITY = new Attribute("city");
            Attribute LINKS = new Attribute("links");

//            for (List<DataSet<Record, Attribute>> allDatasets : ALL_DATASETS) {

//                for (int i = 0; i < allDatasets.size(); i++) {
//                    for (int j = i; j < allDatasets.size(); j++) {


            long timeBlockingMatchingStart = System.currentTimeMillis();

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

//                        System.out.println("\n\n--------------------------------------------------------------------");
//                        System.out.println("R1: " + record1.toString());
//                        System.out.println("R2: " + record2.toString());
//                        System.out.println("score: " + sim);
//
//                        System.out.println("LABEL\tADDRESS\tCITY\tLINKS");
//                        System.out.println(record1.getValue(Organization.ID) + "\t" + record1.getValue(LABEL) + "\t" + record1.getValue(ADDRESS) + "\t" + record1.getValue(CITY) + "\t" + record1.getValue(LINKS) + "\t" + idRecords.get(record1.getValue(Organization.ID)));
//                        System.out.println(record2.getValue(Organization.ID) + "\t" + record2.getValue(LABEL) + "\t" + record2.getValue(ADDRESS) + "\t" + record2.getValue(CITY) + "\t" + record2.getValue(LINKS) + "\t" + idRecords.get(record2.getValue(Organization.ID)));

                    }


                    System.out.println("\n\nNumber of step correspondences: " + correspondences.size());

                }
            }


            long timeBlockingMatchingStop = System.currentTimeMillis();

            long timeBlockingMatching = (timeBlockingMatchingStop - timeBlockingMatchingStart) / 1000;


            System.out.println("TIME BLOCKING-MATCHING: " + timeBlockingMatching);


            if(n_execTimeResolution.containsKey(n_exec)){
                long time = n_execTimeResolution.get(n_exec);

                time = time + timeBlockingMatching;
                n_execTimeResolution.remove(n_exec);
                n_execTimeResolution.put(n_exec,time);


            }else{
                n_execTimeResolution.put(n_exec,timeBlockingMatching);
            }



            long timeClusteringStart = System.currentTimeMillis();

//            }

            System.out.println("\n\n\n NUMBER of ALL correspondences: " + allCorrespondances.size());


//            writeMatches("matches_standard.tsv", allCorrespondances);


            Map<String, Set<Record>> ids = new HashMap<>();


            Set<Set<String>> connectedComponents = new HashSet<>();


            Set<String> allIds = new HashSet<>();

            //Given all the correspondences create a graph
            //Nodes are the entities
            //Edges are the correspondences

            for (Correspondence<Record, Attribute> corr : allCorrespondances) {

                Record r1 = corr.getFirstRecord();
                Record r2 = corr.getSecondRecord();

                String id1 = r1.getValue(Organization.ID);
                String id2 = r2.getValue(Organization.ID);

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


//            BlockCutpointGraph<String, DefaultEdge> blockCutpointGraph = new BlockCutpointGraph<>((Graph<String, DefaultEdge>) g);


            List<Set<String>> connectedSet = c.connectedSets();


            Collections.sort(connectedSet, new Comparator<Set<String>>() {
                @Override
                public int compare(Set<String> o1, Set<String> o2) {
                    return -1 * Integer.compare(o1.size(), o2.size());
                }
            });

            int numberOfComponent = 0;
            int count = 0;
            for (Set<String> set : connectedSet) {


                Graph<String, DefaultEdge> subgraph = new SimpleGraph<>(DefaultEdge.class);
                Set<DefaultEdge> subGraphEdges = new HashSet<>();
                for (String vertex : set) {
                    Set<DefaultEdge> edges = g.edgesOf(vertex);
                    subGraphEdges.addAll(edges);
                    subgraph.addVertex(vertex);
                }

//                Graph<String, DefaultEdge> subgraph = new SimpleGraph<>(DefaultEdge.class);
                for (DefaultEdge edge : subGraphEdges) {
                    String source = g.getEdgeSource(edge);
                    String destination = g.getEdgeTarget(edge);
                    subgraph.addEdge(source, destination);
                }


                System.out.println("-----------------------------------------------------------");

                System.out.println("Vertexes: " + set.size());
                for (String vertex : set) {

                    Record record = idRecords.get(vertex);
                    System.out.println(vertex + "\t" + record.getValue(LABEL) + "\t" + record.getValue(ADDRESS) + "\t" + record.getValue(CITY) + "\t" + record.getValue(LINKS) + "\t" + idSources.get(vertex));
//                    System.out.println(""+ );
                }

                numberOfComponent++;


            }

            List<Set<String>> connectedComponentsCleaned = new ArrayList<>();
            connectedComponentsCleaned.addAll(c.connectedSets());

            System.out.println("Number of connected components: " + connectedComponentsCleaned.size());


            System.out.println("MATCH RESULTS 1: ");

            List<Set<String>> finalMatches = new ArrayList<>();


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
                System.out.println("ID\tLABEL\tADDRESS\tCITY\tLINKS");

                List<Record> records = new ArrayList<>();


                for (String id : set) {
                    Record record = idRecords.get(id);
                    if (record == null) {
                        System.out.println("Null Record!!! ID: " + id);
                    }
                    System.out.println(id + "\t" + record.getValue(LABEL) + "\t" + record.getValue(ADDRESS) + "\t" + record.getValue(CITY) + "\t" + record.getValue(LINKS) + "\t" + idSources.get(id));
                    records.add(record);
                    if (idOtherCandidates.containsKey(id)) {
                        for (String idOther : idOtherCandidates.get(id)) {

                            Record recordExtra = idRecords.get(idOther);
                            if (recordExtra == null) {
                                System.out.println("Null Record!!! ID: " + idOther);
                            }

                            System.out.println("\t\t\t" + idOther + "\t" + recordExtra.getValue(LABEL) + "\t" + recordExtra.getValue(ADDRESS) + "\t" + recordExtra.getValue(CITY) + "\t" + recordExtra.getValue(LINKS) + "\t" + idSources.get(idOther));
                        }
                    }

                }

                // hungarian

                LevenshteinSimilarity lev = new LevenshteinSimilarity();


                Map<Set<Integer>, Double> distancesMap = new HashMap<>();


                double[][] matrix = new double[records.size()][records.size()];
                System.out.println("WEIGHTs ");
                for (int j = 0; j < records.size(); j++) {
                    for (int k = 0; k < records.size(); k++) {

                        Record r1 = records.get(j);
                        Record r2 = records.get(k);

                        String id1 = r1.getValue(Organization.ID);
                        String id2 = r2.getValue(Organization.ID);


                        String label1 = r1.getValue(Organization.LABEL);
                        String label2 = r2.getValue(Organization.LABEL);

                        String city1 = r1.getValue(Organization.CITY);
                        String city2 = r2.getValue(Organization.CITY);


                        String address1 = r1.getValue(Organization.ADDRESS);

                        String address2 = r2.getValue(Organization.ADDRESS);


                        double simAddress = 0.5d;
                        if (address1 != null && address2 != null) {
                            simAddress = lev.calculate(address1, address2);

                        }


                        double simCity = 0.5d;
                        if (city1 != null && city2 != null) {
                            simCity = lev.calculate(city1, city2);

                        }


                        String link1 = r1.getValue(Organization.LINK);

                        String link2 = r2.getValue(Organization.LINK);

                        double simLink = 0.5d;
                        if (link1 != null && link2 != null) {

                            String[] ll1 = link1.split(";");
                            String[] ll2 = link2.split(";");
                            for (String l1 : ll1) {
                                for (String l2 : ll2) {


                                    double localSimLink = lev.calculate(l1, l2);
                                    if (localSimLink > simLink) {
                                        simLink = localSimLink;
                                    }
                                }

                            }

//                            simLink =

                        }


                        List<String> staff1 = RecordComparatorOrganization.parseStaff(r1.getValue(Organization.STAFF));

                        List<String> staff2 = RecordComparatorOrganization.parseStaff(r2.getValue(Organization.STAFF));

                        if (TEST) {
                            System.out.println("SIM LABEL: " + lev.calculate(label1, label2));
                            System.out.println("SIM CITY: " + simCity);
                            System.out.println("SIM ADDRESS: " + simAddress);
                            System.out.println("SIM LINK: " + simLink);
                        }


                        double sim = 0.4d * lev.calculate(label1, label2) + 0.4d * simCity + 0.1d * simAddress + 0.1d * simLink;

                        // add match possibility
                        for (String person : staff1) {
                            if (staff2.contains(person)) {
                                sim = 1.0d;
                            }
                        }


                        String source1 = idSources.get(id1);
                        String source2 = idSources.get(id2);

                        double weight = Double.MAX_VALUE;

                        if (!source1.equals(source2)) {


                            // modify here

                            if (sim < 0.5d) {

                            } else {
                                weight = 1 - sim;
                            }
                        }


                        matrix[j][k] = weight;
                        Set<Integer> point = new HashSet<>();
                        point.add(j);
                        point.add(k);
                        distancesMap.put(point, weight);

                        if (weight == Double.MAX_VALUE) {
                            System.out.println(j + "\t\t" + k + "\t\t" + "MAX" + "\t\tSIM: " + sim);
                        } else {
                            System.out.println(j + "\t\t" + k + "\t\t" + weight + "\t\tSIM: " + sim);
                        }


                    }
                }


                List<Map.Entry<Set<Integer>, Double>> distancesList = new ArrayList<>();
                distancesList.addAll((Collection<? extends Map.Entry<Set<Integer>, Double>>) distancesMap.entrySet());


                Collections.sort(distancesList, new Comparator<Map.Entry<Set<Integer>, Double>>() {
                    @Override
                    public int compare(Map.Entry<Set<Integer>, Double> o1, Map.Entry<Set<Integer>, Double> o2) {
                        return o1.getValue().compareTo(o2.getValue());
                    }
                });

                System.out.println("Weight ordered and source ordered");


                for (Map.Entry<Set<Integer>, Double> entry : distancesList) {


                    System.out.println(entry.getKey().toString() + "\t" + entry.getValue());

                }


                Graph<String, DefaultEdge> matchGraph1 = new SimpleGraph<>(DefaultEdge.class);

                Set<Integer> usedVertex = new HashSet<>();
//                Set<String> vertexes =
                for (Map.Entry<Set<Integer>, Double> entry : distancesList) {


                    List<Integer> iiiiii = new ArrayList<>();
                    iiiiii.addAll(entry.getKey());


                    System.out.println("\tInput distances: " + iiiiii.toString());

                    if (iiiiii.size() != 2) {
                        System.out.println("\t\tSame nodes");
                        continue;
                    }

                    if (entry.getValue().doubleValue() == Double.MAX_VALUE) {
                        System.out.println("\t\tMAX DISTANCE");
                        continue;
                    }

                    Integer aa = iiiiii.get(0);
                    Integer bb = iiiiii.get(1);


                    Graph<String, DefaultEdge> matchGraphCandidate = new SimpleGraph<>(DefaultEdge.class);


                    for (DefaultEdge e : matchGraph1.edgeSet()) {

                        String source = matchGraph1.getEdgeSource(e);

                        String dest = matchGraph1.getEdgeTarget(e);


                        matchGraphCandidate.addVertex(source);
                        matchGraphCandidate.addVertex(dest);
                        matchGraphCandidate.addEdge(source, dest);

                    }


                    String leftCandidate = records.get(aa).getValue(Organization.ID);
                    String rightCandidate = records.get(bb).getValue(Organization.ID);

                    matchGraphCandidate.addVertex(leftCandidate);
                    matchGraphCandidate.addVertex(rightCandidate);
                    matchGraphCandidate.addEdge(leftCandidate, rightCandidate);


                    ConnectivityInspector<String, DefaultEdge> inspector = new ConnectivityInspector<>(matchGraphCandidate);


                    boolean sourceViolations = false;


                    for (Set<String> connectedComponent : inspector.connectedSets()) {

                        Set<String> types = new HashSet<>();
                        for (String element : connectedComponent) {


                            String type = idSources.get(element);

                            if (!type.equals("orcid")) {
                                if (types.contains(type)) {
                                    sourceViolations = true;

                                } else {
                                    types.add(type);
                                }
                            } else {
                                types.add(type);
                            }


                        }

                    }


//                    if (usedVertex.contains(aa) && usedVertex.contains(bb)) {
                    if (sourceViolations) {
//                        System.out.println("\t\tUSED VERTEX");
                        System.out.println("\t\tSOURCE VIOLATION");
                    } else {


                        String left = records.get(aa).getValue(Organization.ID);
                        String right = records.get(bb).getValue(Organization.ID);


//                        Set<DefaultEdge> edgesLeft = matchGraph1.edgesOf(left);
//                        Set<DefaultEdge> edgesRight = matchGraph1.edgesOf(right);


                        ConnectivityInspector<String, DefaultEdge> cinsp1 = new ConnectivityInspector<>(matchGraph1);

                        boolean violation = false;

                        if (matchGraph1.containsVertex(left)) {

                            for (String vertex : cinsp1.connectedSetOf(left)) {
                                Set<Integer> pair = new HashSet<>();
                                pair.add(records.indexOf(idRecords.get(vertex)));
                                pair.add(records.indexOf(idRecords.get(left)));


                                if (vertex.equals(left)) {
                                    continue;

                                }


                                if (distancesMap.containsKey(pair)) {

                                    if (distancesMap.get(pair).doubleValue() == Double.MAX_VALUE) {
                                        System.out.println("\t\tVIOLATION: PAIR " + pair.toString() + "V1: " + vertex + " V2: " + left);
                                        violation = true;
                                    }

                                } else {
                                    System.err.println("ERROR DISTANCE IS NOT PRESENT");
                                    System.exit(0);
                                }

                            }
                        }

                        if (matchGraph1.containsVertex(right)) {
                            for (String vertex : cinsp1.connectedSetOf(right)) {
                                Set<Integer> pair = new HashSet<>();
                                pair.add(records.indexOf(idRecords.get(vertex)));
                                pair.add(records.indexOf(idRecords.get(right)));

                                if (vertex.equals(right)) {
                                    continue;

                                }

                                if (distancesMap.containsKey(pair)) {

                                    if (distancesMap.get(pair).doubleValue() == Double.MAX_VALUE) {
                                        System.out.println("\t\tVIOLATION: PAIR " + pair.toString() + "V1: " + vertex + " V2: " + right);
                                        violation = true;
                                    }

                                } else {
                                    System.err.println("ERROR DISTANCE IS NOT PRESENT");
                                    System.exit(0);
                                }
                            }
                        }

                        if (!violation) {


                            matchGraph1.addVertex(left);
                            matchGraph1.addVertex(right);
                            matchGraph1.addEdge(left, right);

                            usedVertex.add(aa);
                            usedVertex.add(bb);
                        } else {
                            System.out.println("\t\tVIOLATION");
                        }
                    }

                }


                System.out.println("Records: ");

                for (int k = 0; k < records.size(); k++) {
                    Record record = records.get(k);
                    System.out.println(k + "\t" + record.getValue(Organization.ID) + "\t" + record.getValue(LABEL) + "\t" + record.getValue(ADDRESS) + "\t" + record.getValue(CITY) + "\t" + record.getValue(LINKS) + "\t" + idSources.get(record.getValue(Organization.ID)));
                }


//
//
                List<Set<String>> localMatches = new ArrayList<>();
//
//


                System.out.println("LOCAL MATCHES MIN ");

                printLocalMatches(matchGraph1, idRecords, idSources, idOtherCandidates, set);


                System.out.println("=======================================================================");
                System.out.println("=======================================================================");
                System.out.println("=======================================================================");

                System.out.println("LOCAL MATCHES MIN ");
//

                ConnectivityInspector<String, DefaultEdge> cinsp = new ConnectivityInspector<>(matchGraph1);

                localMatches.addAll(cinsp.connectedSets());

                for (Set<String> matches : localMatches) {
                    System.out.println("" + matches.toString());
                }

                Set<String> recordMatches = new HashSet<>();
                for (Set<String> matches : localMatches) {
                    System.out.println("----------------------------------------------------------");

                    Set<String> match = new HashSet<>();

                    for (String id : matches) {
                        recordMatches.add(id);
                        Record record = idRecords.get(id);
                        System.out.println(record.getValue(Organization.ID) + "\t" + record.getValue(LABEL) + "\t" + record.getValue(ADDRESS) + "\t" + record.getValue(CITY) + "\t" + record.getValue(LINKS) + "\t" + idSources.get(record.getValue(Organization.ID)));
                        match.add(id);
                        if (idOtherCandidates.containsKey(id)) {

                            match.addAll(idOtherCandidates.get(id));
                            for (String idOther : idOtherCandidates.get(id)) {
                                Record recordExtra = idRecords.get(idOther);
                                if (recordExtra == null) {
                                    System.out.println("Null Record!!! ID: " + idOther);
                                }
                                match.add(idOther);
                                System.out.println("\t\t\t" + idOther + "\t" + recordExtra.getValue(LABEL) + "\t" + recordExtra.getValue(ADDRESS) + "\t" + recordExtra.getValue(CITY) + "\t" + recordExtra.getValue(LINKS) + "\t" + idSources.get(idOther));
                            }

                        }

                    }


                    finalMatches.add(match);
                }

                Set<String> recordSetRemaing = new HashSet<>();

                recordSetRemaing.addAll(set);
                recordSetRemaing.removeAll(recordMatches);

                for (String id : recordSetRemaing) {

                    Set<String> match = new HashSet<>();
                    System.out.println("----------------------------------------------------------");

                    Record record = idRecords.get(id);
                    System.out.println(record.getValue(Organization.ID) + "\t" + record.getValue(LABEL) + "\t" + record.getValue(ADDRESS) + "\t" + record.getValue(CITY) + "\t" + record.getValue(LINKS) + "\t" + idSources.get(record.getValue(Organization.ID)));
                    match.add(id);
                    if (idOtherCandidates.containsKey(id)) {
                        match.addAll(idOtherCandidates.get(id));
                        for (String idOther : idOtherCandidates.get(id)) {

                            Record recordExtra = idRecords.get(idOther);
                            if (recordExtra == null) {
                                System.out.println("Null Record!!! ID: " + idOther);
                            }
                            match.add(idOther);
                            System.out.println("\t\t\t" + idOther + "\t" + recordExtra.getValue(LABEL) + "\t" + recordExtra.getValue(ADDRESS) + "\t" + recordExtra.getValue(CITY) + "\t" + recordExtra.getValue(LINKS) + "\t" + idSources.get(idOther));
                        }
                    }
                    finalMatches.add(match);

                }


                ///add extra matches


            }
            long timeClusteringStop = System.currentTimeMillis();


            long timeClustering = (timeClusteringStop - timeClusteringStart) / 1000;

            System.out.println("TIME CLUSTERING: " + timeClustering);


            if(n_execTimeClustering.containsKey(n_exec)){
                long time = n_execTimeClustering.get(n_exec);

                time = time + timeClustering;
                n_execTimeClustering.remove(n_exec);
                n_execTimeClustering.put(n_exec,time);


            }else{
                n_execTimeClustering.put(n_exec,timeClustering);
            }

//            writeCorrespondence("correspondenceOrganizations_" + mode + "_standard.tsv", finalMatches);


            System.out.println("Founded " + i + " correspondences.");

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.exit(0);

        }

        long end = System.currentTimeMillis();

        long time = (end - start) / 1000;


        System.out.println("RESOLUTION TIME NEEDED: " + time);

    }


    public static void writeMatches(String filename, Collection<Correspondence<Record, Attribute>> allCorrespondances) {
        BufferedWriter writer = null;

//        System.out.println("Number of edges: " + edges.size());
        try {
            writer = new BufferedWriter(new FileWriter(filename));
            writer.write("graph {\n");

            for (Correspondence<Record, Attribute> corr : allCorrespondances) {


                writer.write(corr.getFirstRecord().getValue(Organization.ID) + "\t" + corr.getSecondRecord().getValue(Organization.ID) + "\t" + corr.getSimilarityScore() + "\n");


            }
            writer.write("}");

            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static void printLocalMatches(Graph<String, DefaultEdge> matchGraph, Map<String, Record> idRecords, Map<String, String> idSources, Map<String, Set<String>> idOtherCandidates, Set<String> recordSet) {
        ConnectivityInspector<String, DefaultEdge> cinsp = new ConnectivityInspector<>(matchGraph);
        List<Set<String>> localMatches = new ArrayList<>();
        localMatches.addAll(cinsp.connectedSets());

        for (Set<String> matches : localMatches) {
            System.out.println("" + matches.toString());
        }

        Set<String> recordMatches = new HashSet<>();
        for (Set<String> matches : localMatches) {
            System.out.println("----------------------------------------------------------");

            Set<String> match = new HashSet<>();

            for (String id : matches) {
                recordMatches.add(id);
                Record record = idRecords.get(id);
                if (record == null) {
                    System.err.println("NULL ID: " + id);
                    continue;
                }

                System.out.println(record.getValue(Organization.ID) + "\t" + record.getValue(Organization.LABEL) + "\t" + record.getValue(Organization.ADDRESS) + "\t" + record.getValue(Organization.CITY) + "\t" + record.getValue(Organization.LINK) + "\t" + idSources.get(record.getValue(Organization.ID)));
                match.add(id);
                if (idOtherCandidates.containsKey(id)) {
                    match.addAll(idOtherCandidates.get(id));
                    for (String idOther : idOtherCandidates.get(id)) {

                        Record recordExtra = idRecords.get(idOther);
                        if (recordExtra == null) {
                            System.out.println("Null Record!!! ID: " + idOther);
                        }
                        System.out.println("\t\t\t" + idOther + "\t" + recordExtra.getValue(Organization.LABEL) + "\t" + recordExtra.getValue(Organization.ADDRESS) + "\t" + recordExtra.getValue(Organization.CITY) + "\t" + recordExtra.getValue(Organization.LINK) + "\t" + idSources.get(idOther));
                    }
                }

            }


        }

        Set<String> recordSetRemaing = new HashSet<>();

        recordSetRemaing.addAll(recordSet);
        recordSetRemaing.removeAll(recordMatches);

        for (String id : recordSetRemaing) {

            Set<String> match = new HashSet<>();
            System.out.println("----------------------------------------------------------");

            Record record = idRecords.get(id);
            System.out.println(record.getValue(Organization.ID) + "\t" + record.getValue(Organization.LABEL) + "\t" + record.getValue(Organization.ADDRESS) + "\t" + record.getValue(Organization.CITY) + "\t" + record.getValue(Organization.LINK) + "\t" + idSources.get(record.getValue(Organization.ID)));
            match.add(id);
            if (idOtherCandidates.containsKey(id)) {
                match.addAll(idOtherCandidates.get(id));
                for (String idOther : idOtherCandidates.get(id)) {

                    Record recordExtra = idRecords.get(idOther);
                    if (recordExtra == null) {
                        System.out.println("Null Record!!! ID: " + idOther);
                    }
                    System.out.println("\t\t\t" + idOther + "\t" + recordExtra.getValue(Organization.LABEL) + "\t" + recordExtra.getValue(Organization.ADDRESS) + "\t" + recordExtra.getValue(Organization.CITY) + "\t" + recordExtra.getValue(Organization.LINK) + "\t" + idSources.get(idOther));
                }
            }

        }
    }


    public static int getNullValues(Record r) {

        int nullValues = 0;

        List<Attribute> atts = new ArrayList<>();
        atts.add(Organization.LABEL);
        atts.add(Organization.CITY);
        atts.add(Organization.ADDRESS);
        atts.add(Organization.LINK);


        for (Attribute att : atts) {
            if (r.hasValue(att)) {
                String value = r.getValue(att);
                if (value == null) {
                    nullValues++;
                }
            } else {
                nullValues++;
            }
        }
        return nullValues;
    }


    public static DataSet<Record, Attribute> filterDatasetByCountryCode(DataSet<Record, Attribute> input, String countryCode) {


        DataSet<Record, Attribute> datasetFiltered = new HashedDataSet<>();
        for (Record r : input.get()) {
            String cc = r.getValue(Organization.COUNTRY_CODE);
            if (countryCode.equals(cc)) {
                datasetFiltered.add(r);
            }
        }

        return datasetFiltered;

    }

    public static DataSet<Record, Attribute> filterDatasetByCountryCode(DataSet<Record, Attribute> input, String countryCode, Set<String> itemToRemove) {


        DataSet<Record, Attribute> datasetFiltered = new HashedDataSet<>();
        for (Record r : input.get()) {
            String cc = r.getValue(Organization.COUNTRY_CODE);
            if (countryCode.equals(cc)) {
                if (!itemToRemove.contains(r.getValue(Organization.ID))) {
                    datasetFiltered.add(r);
                }
            }
        }

        return datasetFiltered;

    }


    public static DataSet<Record, Attribute> filterDatasetByIds(DataSet<Record, Attribute> input, Set<String> itemToRemove) {


        DataSet<Record, Attribute> datasetFiltered = new HashedDataSet<>();
        for (Record r : input.get()) {

            if (!itemToRemove.contains(r.getValue(Organization.ID))) {
                datasetFiltered.add(r);
            }

        }

        return datasetFiltered;

    }


    public static DataSet<Record, Attribute> filterDatasetByEntityIds(DataSet<Record, Attribute> input, Set<String> itemToMantain) {


        DataSet<Record, Attribute> datasetFiltered = new HashedDataSet<>();
        for (Record r : input.get()) {

            if (itemToMantain.contains(r.getValue(Organization.ID))) {
                datasetFiltered.add(r);
            }

        }

        return datasetFiltered;

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
