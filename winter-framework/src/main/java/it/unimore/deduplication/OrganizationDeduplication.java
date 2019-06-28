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
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import it.unimore.comparators.RecordComparatorOrganization;
import it.unimore.deduplication.model.Organization;
import org.apache.jena.base.Sys;
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


public class OrganizationDeduplication {


    private Set<Set<String>> biconnectedComponents;

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

        DataSet<Record, Attribute> csv = new HashedDataSet<>();
        DataSet<Record, Attribute> gerit = new HashedDataSet<>();
        // /Users/paolosottovia/Downloads/extracted_data_17_03_2018/

//        String basePath = "/Users/paolosottovia/Downloads/extracted_data_17_03_2018/";

        String basePath = arg[0];
        String mode = arg[1];
        String test = arg[2];
        String filterS = null;
        boolean filterSource = false;
        if (arg.length > 3) {
            filterS = arg[3];

        }

        System.out.println("BASEPATH: " + basePath);
        System.out.println("MODE: " + mode);


        if (filterS != null) {
            if (filterS.equals("test")) {
                filterSource = true;
            }
        }

//        String testString ="65166\t44985\t45992\t74811\t5101\t46783\t7589\t56296\t53382\t65968\t38181\t27702\t171996\t178809\t177619\t181090\t79317\t26035\t77953\t37995\t178348\t174027\t178246\t32\t154671\t74406\t17350\t132843\t74965\t40550\t66368\t27513\t71551\t43849\t126067\t5192\t27516\t39062\t33369\t79722\t77642\t32772\t54225";


//        String testString = "188447\t104428";

        //String testString = "58554\t73632\t77821\t191954\t35231\t51565\t31360\t58562\t78064\t29040\t58580";


        //String testString = "1205\t296912\t19352\t19384\t19340\t19516\t110767";
        // String testString ="296912\t110767";

        // String testString = "6560\t70566\t7787\t42110\t49533\t41078\t77583\t276\t2514\t167654\t55539\t14886\t439\t178344\t191636\t410264\t397427\t291102";

        //String testString = "37370\t18973\t403696\t40286\t95795\t29926\t40966\t651\t33111\t60472\t45011\t56719\t77\t68641\t49009\t74920\t63673\t4223\t4421\t402676\t17096\t58085\t52266\t46451\t413010";


        //String testString ="293132\t95795";
        //String testString = "291997\t112848\t112849\t112846";

        //String testString = "80395\t291997";


        String testString = "175527\t395711";


        Set<String> idsToFilter = new HashSet<>();
        for (String item : testString.split("\t")) {
            idsToFilter.add(item);
        }


        Set<String> sourceLabelToMantain = new HashSet<>();


        sourceLabelToMantain.add("grid");
        sourceLabelToMantain.add("scanr");


        if (test.equals("test")) {
            TEST = true;
        }


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

        File geritFile = new File(basePath + "organizations_Gerit.csv");


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
                    geritFile, gerit);

        } catch (IOException e) {
            e.printStackTrace();
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
        cleaner.cleanDB(gerit);
        if (includeOrcid) {
            cleaner.cleanDB(orcid);
            cleaner.cleanDB(orcidParent);
        }


        // List<DataSet> listOfDataset = new ArrayList<>();


        Map<String, DataSet> DATASETS = new HashMap<>();


        int baseUrlCountThreshold = 10;


//listOfDataset.add(openaire);
        DATASETS.put("openaire", openaire);
        //listOfDataset.add(cercauniversita);
        DATASETS.put("cercauniversita", cercauniversita);
        //listOfDataset.add(arianna);
        DATASETS.put("arianna", arianna);
        // listOfDataset.add(questio);
        DATASETS.put("questio", questio);
        //listOfDataset.add(bvd);
        DATASETS.put("bvd", bvd);
        //listOfDataset.add(cnr);
        DATASETS.put("cnr", cnr);
        //listOfDataset.add(scanr);
        DATASETS.put("scanr", scanr);
        //listOfDataset.add(registroImprese);
        DATASETS.put("registroImprese", registroImprese);
        //listOfDataset.add(patiris);
        DATASETS.put("patiris", patiris);
        // listOfDataset.add(orcid);
        DATASETS.put("orcid", orcid);
        //listOfDataset.add(orcidParent);
        //listOfDataset.add(p3);
        DATASETS.put("p3", p3);
        //listOfDataset.add(grid);
        DATASETS.put("grid", grid);
        //listOfDataset.add(fwf);
        DATASETS.put("fwf", fwf);
        // listOfDataset.add(sicris);
        DATASETS.put("sicris", sicris);
        //listOfDataset.add(gerit);
        DATASETS.put("gerit", gerit);

        List<String> baseUrlToExclude = retrieveFrequentUrlBases(DATASETS, baseUrlCountThreshold);


        DATASETS.clear();

        if (TEST) {


//            filterDatasetByEntityIds

            openaire = filterDatasetByEntityIds(openaire, idsToFilter);
            cercauniversita = filterDatasetByEntityIds(cercauniversita, idsToFilter);
            arianna = filterDatasetByEntityIds(arianna, idsToFilter);
            questio = filterDatasetByEntityIds(questio, idsToFilter);
            bvd = filterDatasetByEntityIds(bvd, idsToFilter);
            cnr = filterDatasetByEntityIds(cnr, idsToFilter);
            scanr = filterDatasetByEntityIds(scanr, idsToFilter);
            registroImprese = filterDatasetByEntityIds(registroImprese, idsToFilter);
            patiris = filterDatasetByEntityIds(patiris, idsToFilter);
            orcid = filterDatasetByEntityIds(orcid, idsToFilter);
            orcidParent = filterDatasetByEntityIds(orcidParent, idsToFilter);
            p3 = filterDatasetByEntityIds(p3, idsToFilter);
            grid = filterDatasetByEntityIds(grid, idsToFilter);
            fwf = filterDatasetByEntityIds(fwf, idsToFilter);
            sicris = filterDatasetByEntityIds(sicris, idsToFilter);
            gerit = filterDatasetByEntityIds(gerit, idsToFilter);


        }

        if (filterSource) {
            if (sourceLabelToMantain.contains("openaire")) {
                //listOfDataset.add(openaire);
                DATASETS.put("openaire", openaire);
            }
            if (sourceLabelToMantain.contains("cercauniversita")) {
                //listOfDataset.add(cercauniversita);
                DATASETS.put("cercauniversita", cercauniversita);
            }
            if (sourceLabelToMantain.contains("arianna")) {
                //listOfDataset.add(arianna);
                DATASETS.put("arianna", arianna);

            }
            if (sourceLabelToMantain.contains("questio")) {
                //listOfDataset.add(questio);
                DATASETS.put("questio", questio);
            }
            if (sourceLabelToMantain.contains("bvd")) {
                //listOfDataset.add(bvd);
                DATASETS.put("bvd", bvd);

            }
            if (sourceLabelToMantain.contains("cnr")) {
                //listOfDataset.add(cnr);

                DATASETS.put("cnr", cnr);

            }
            if (sourceLabelToMantain.contains("scanr")) {
                //listOfDataset.add(scanr);
                DATASETS.put("scanr", scanr);

            }
            if (sourceLabelToMantain.contains("registroImprese")) {
                //listOfDataset.add(registroImprese);
                DATASETS.put("registroImprese", registroImprese);

            }
            if (sourceLabelToMantain.contains("patiris")) {
                // listOfDataset.add(patiris);
                DATASETS.put("patiris", patiris);

            }
            if (sourceLabelToMantain.contains("orcid")) {
                // listOfDataset.add(orcid);
                DATASETS.put("orcid", orcid);

            }
            if (sourceLabelToMantain.contains("p3")) {
                //listOfDataset.add(p3);
                DATASETS.put("p3", p3);

            }
            if (sourceLabelToMantain.contains("grid")) {
                //listOfDataset.add(grid);
                DATASETS.put("grid", grid);
            }
            if (sourceLabelToMantain.contains("fwf")) {
                //listOfDataset.add(fwf);
                DATASETS.put("fwf", fwf);
            }
            if (sourceLabelToMantain.contains("grid")) {
                //listOfDataset.add(grid);
                DATASETS.put("grid", grid);
            }
            if (sourceLabelToMantain.contains("sicris")) {
                //listOfDataset.add(sicris);
                DATASETS.put("sicris", sicris);
            }
            if (sourceLabelToMantain.contains("gerit")) {
                //listOfDataset.add(gerit);
                DATASETS.put("gerit", gerit);
            }


        } else {
            //listOfDataset.add(openaire);
            DATASETS.put("openaire", openaire);
            //listOfDataset.add(cercauniversita);
            DATASETS.put("cercauniversita", cercauniversita);
            //listOfDataset.add(arianna);
            DATASETS.put("arianna", arianna);
            // listOfDataset.add(questio);
            DATASETS.put("questio", questio);
            //listOfDataset.add(bvd);
            DATASETS.put("bvd", bvd);
            //listOfDataset.add(cnr);
            DATASETS.put("cnr", cnr);
            //listOfDataset.add(scanr);
            DATASETS.put("scanr", scanr);
            //listOfDataset.add(registroImprese);
            DATASETS.put("registroImprese", registroImprese);
            //listOfDataset.add(patiris);
            DATASETS.put("patiris", patiris);
            // listOfDataset.add(orcid);
            DATASETS.put("orcid", orcid);
            //listOfDataset.add(orcidParent);
            //listOfDataset.add(p3);
            DATASETS.put("p3", p3);
            //listOfDataset.add(grid);
            DATASETS.put("grid", grid);
            //listOfDataset.add(fwf);
            DATASETS.put("fwf", fwf);
            // listOfDataset.add(sicris);
            DATASETS.put("sicris", sicris);
            //listOfDataset.add(gerit);
            DATASETS.put("gerit", gerit);
        }


        System.out.println("\nNumber of items in Openaire: " + openaire.size());
        System.out.println("Number of items in cerca universita: " + cercauniversita.size());
        System.out.println("Number of items in questio: " + questio.size());
        System.out.println("Number of items in arianna: " + arianna.size());
        System.out.println("Number of items in bvd: " + bvd.size());
        System.out.println("Number of items in cnr: " + cnr.size());
        System.out.println("Number of items in scanr: " + scanr.size());
        System.out.println("Number of items in registro imprese: " + registroImprese.size());
        System.out.println("Number of items in patiris: " + patiris.size());
        System.out.println("Number of items in p3: " + p3.size());
        System.out.println("Number of items in grid: " + grid.size());
        System.out.println("Number of items in fwf: " + fwf.size());
        System.out.println("Number of items in sicris: " + sicris.size());
        System.out.println("Number of items in gerit: " + gerit.size());


        System.out.println("Number of DATASETS selected: " + DATASETS.size());


        //System.exit(0);


        includeOrcid = DATASETS.containsKey("orcid");
        if (includeOrcid) {
            System.out.println("Number of items in orcid: " + orcid.size());
            System.out.println("Number of items in orcid parent: " + orcidParent.size());
        }

        if (TEST) {
            System.out.println("===============================================================");

            for (DataSet<Record, Attribute> dataset : DATASETS.values()) {
                for (Record record : dataset.get()) {
                    System.out.println("\t" + record.getValue(Organization.ID) + "\t" + record.getValue(Organization.LABEL) + "\t" + record.getValue(Organization.ADDRESS) + "\t" + record.getValue(Organization.CITY) + "\t" + record.getValue(Organization.LINK));


                }
            }
            System.out.println("===============================================================");
        }

        //System.exit(0);


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
        DataSet<Record, Attribute> openAireFiltered = null;


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
        List<Set<String>> matchesOpenAire = new ArrayList<>();

        if (DATASETS.containsKey("openaire")) {
            OrganizationSingleDatasetResolution OSDR = new OrganizationSingleDatasetResolution();

            cleaner.cleanDB(openaire);

            matchesOpenAire = OSDR.runIdentityResolution(openaire);

        }

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


        openAireFiltered = filterDatasetByIds(openaire, openaireIdToRemove);


        System.out.println("OPENAIRE FILTERED:");

        for (Record record : openAireFiltered.get()) {

            System.out.println("\t" + record.getValue(Organization.ID) + "\t" + record.getValue(Organization.LABEL) + "\t" + record.getValue(Organization.ADDRESS) + "\t" + record.getValue(Organization.CITY) + "\t" + record.getValue(Organization.LINK));


        }
        System.out.println("END OPENAIRE!!!");
        System.out.println("Number of matches openaire: " + matchesOpenAire.size());
        System.out.println("==============END DEDUPLICATION OF OPENAIRE=================\n\n\n\n");


        //Change here


        if (DATASETS.containsKey("openaire")) {
            DATASETS.put("openaire", openAireFiltered);
        }


        Map<String, String> idSources = new HashMap<>();

        //or (int i = 0; i < dataSets.size(); i++) {

        for (String datasetName : DATASETS.keySet()) {

            DataSet<Record, Attribute> data = DATASETS.get(datasetName);


            for (Record record : data.get()) {

                String id = record.getValue(Organization.ID);

                idSources.put(id, datasetName);


            }

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
                blocker.setMeasureBlockSizes(true);


                break;
            default:
                System.exit(0);
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


        /////
        ////
        ///
        //


//
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

        // ORCID


        //FIX ORCID
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

        //


        try {


            RecordComparatorLevenshtein city = new RecordComparatorLevenshtein(Organization.CITY, Organization.CITY);
            city.setLowerCase(true);

            RecordComparatorLevenshtein link = new RecordComparatorLevenshtein(Organization.LINK, Organization.LINK);
            link.setLowerCase(true);

            RecordComparatorOrganization organizationComparator = new RecordComparatorOrganization(Organization.LABEL, Organization.LABEL);
            organizationComparator.setBaseUrlToExlude(baseUrlToExclude);
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

            //           for (int i = 0; i < dataSets.size(); i++) {
            //               for (int j = i; j < dataSets.size(); j++) {


            List<String> datasetNames = new ArrayList<>();
            datasetNames.addAll(DATASETS.keySet());

            for (int i = 0; i < datasetNames.size(); i++) {
                for (int j = i; j < datasetNames.size(); j++) {


                    long timeStampStart = System.currentTimeMillis();
                    if (i == j) {
                        continue;
                    }

                    String datasetName1 = datasetNames.get(i);
                    String datasetName2 = datasetNames.get(j);

                    DataSet<Record, Attribute> dataSet1 = DATASETS.get(datasetName1);
                    DataSet<Record, Attribute> dataSet2 = DATASETS.get(datasetName2);

                    // skip not useful comparison
                    if ((dataSet1.size() == 0) || (dataSet2.size() == 0)) {
                        continue;
                    }
                    System.out.println("DATASET; " + datasetName1 + "\tDataset1 size: " + dataSet1.size());
                    System.out.println("DATASET; " + datasetName2 + "\tDataset2 size: " + dataSet2.size());

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

                    long timeStampStop = System.currentTimeMillis();

                    long totalTime = (timeStampStop - timeStampStart) / 1000;


                    System.out.println("------------------------REPORT------------------------");
                    System.out.println("DATASET; " + datasetName1 + "\t size: " + dataSet1.size());
                    System.out.println("DATASET; " + datasetName2 + "\t size: " + dataSet2.size());
                    System.out.println("Time needed: " + totalTime + "s");
                    System.out.println("\n\nNumber of step correspondences: " + correspondences.size());

                }
            }


//            }

            System.out.println("\n\n\n NUMBER of ALL correspondences: " + allCorrespondances.size());


            writeMatches("matches.tsv", allCorrespondances);


            Map<String, Set<Record>> ids = new HashMap<>();


            Set<Set<String>> connectedComponents = new HashSet<>();


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


            System.out.println("MATCH RESULTS: ");

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
//                            records.add(recordExtra);

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
//                                    if(TEST){
//                                        System.out.println("L1: "+l1);
//                                        System.out.println("L2: "+l2);
//                                        System.out.println("SIM: "+localSimLink);
//                                    }
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
                            //System.out.println(j + "\t\t" + k + "\t\t" + "MAX" + "\t\tSIM: " + sim);
                        } else {
                            //System.out.println(j + "\t\t" + k + "\t\t" + weight + "\t\tSIM: " + sim);
                        }

                        // System.out.println("END COMPARISON!");


                    }
                }

                if (i % 100 == 0) {
                    System.out.println("Processed: " + i + "/" + connectedComponentsCleaned.size());
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


            writeCorrespondence("correspondenceOrganizations_" + mode + ".tsv", finalMatches);


            System.out.println("Founded " + i + " correspondences.");

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.exit(0);

        }


    }


    public static List<String> retrieveFrequentUrlBases(Map<String, DataSet> DATASETS, int baseUrlCountThreshold) {
        List<Map.Entry<String, Integer>> linksCounts = new ArrayList<>();
        Map<String, Integer> link_count = new HashMap<>();

        for (DataSet<Record, Attribute> dataset : DATASETS.values()) {
            for (Record record : dataset.get()) {
                String linkEntry = record.getValue(Organization.LINK);

                List<String> links = RecordComparatorOrganization.parseLink(linkEntry, false);
                for (String link : links) {
                    if (link_count.containsKey(link)) {
                        int count = link_count.get(link);
                        count = count + 1;
                        link_count.remove(link);
                        link_count.put(link, count);
                    } else {
                        link_count.put(link, 1);
                    }
                }


            }
        }

        linksCounts.addAll(link_count.entrySet());

        Collections.sort(linksCounts, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return -1 * o1.getValue().compareTo(o2.getValue());
            }
        });


        System.out.println("LINK\tCount");
        for (Map.Entry<String, Integer> item : linksCounts) {
            System.out.println(item.getKey() + "\t" + item.getValue());
        }

        List<String> baseUrls = new ArrayList<>();
        for (Map.Entry<String, Integer> item : linksCounts) {
            if (item.getValue().intValue() >= baseUrlCountThreshold) {
                baseUrls.add(item.getKey());
            }
        }


        System.out.println("Number of excluded base urls: " + baseUrls.size());

        return baseUrls;
    }

    public static void writeMatches(String
                                            filename, Collection<Correspondence<Record, Attribute>> allCorrespondances) {
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


    private static void printLocalMatches
            (Graph<String, DefaultEdge> matchGraph, Map<String, Record> idRecords, Map<String, String> idSources, Map<String, Set<String>> idOtherCandidates, Set<String> recordSet) {
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


    public static DataSet<Record, Attribute> filterDatasetByCountryCode(DataSet<Record, Attribute> input, String
            countryCode) {


        DataSet<Record, Attribute> datasetFiltered = new HashedDataSet<>();
        for (Record r : input.get()) {
            String cc = r.getValue(Organization.COUNTRY_CODE);
            if (countryCode.equals(cc)) {
                datasetFiltered.add(r);
            }
        }

        return datasetFiltered;

    }

    public static DataSet<Record, Attribute> filterDatasetByCountryCode(DataSet<Record, Attribute> input, String
            countryCode, Set<String> itemToRemove) {


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


    public static DataSet<Record, Attribute> filterDatasetByIds
            (DataSet<Record, Attribute> input, Set<String> itemToRemove) {


        DataSet<Record, Attribute> datasetFiltered = new HashedDataSet<>();
        for (Record r : input.get()) {

            if (!itemToRemove.contains(r.getValue(Organization.ID))) {
                datasetFiltered.add(r);
            }

        }

        return datasetFiltered;

    }


    public static DataSet<Record, Attribute> filterDatasetByEntityIds
            (DataSet<Record, Attribute> input, Set<String> itemToMantain) {


        DataSet<Record, Attribute> datasetFiltered = new HashedDataSet<>();
        for (Record r : input.get()) {

            if (itemToMantain.contains(r.getValue(Organization.ID))) {
                datasetFiltered.add(r);
            }

        }

        return datasetFiltered;

    }


    public static void printGraph(String basepath, int filenumber, Graph<
            String, DefaultEdge> graph, Set<DefaultEdge> edges, Map<String, Record> idRecord) {


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
