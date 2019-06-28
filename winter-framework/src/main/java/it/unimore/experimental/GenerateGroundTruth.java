package it.unimore.experimental;

import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.CSVRecordReader;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import it.unimore.deduplication.OrganizationDBCleaner;
import it.unimore.deduplication.model.Organization;
import org.apache.jena.base.Sys;

import java.io.*;
import java.util.*;

public class GenerateGroundTruth {

    public static void main(String arg[]) {

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


        System.out.println("BASEPATH: " + basePath);

        boolean includeOrcid = true;

        boolean GENERATE_SAMPLE = false;


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

        } catch (
                IOException e) {
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


        if (includeOrcid) {
            System.out.println("Number of items in orcid: " + orcid.size());

            System.out.println("Number of items in orcid parent: " + orcidParent.size());
        }
        List<List<DataSet<Record, Attribute>>> ALL_DATASETS = new ArrayList<>();


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
        if (includeOrcid) {
            cleaner.cleanDB(orcid);
            cleaner.cleanDB(orcidParent);
        }


        List<DataSet<Record, Attribute>> dataSets = new ArrayList<>();
        dataSets.add(openaire);
        dataSets.add(cercauniversita);
        dataSets.add(questio);
        dataSets.add(arianna);
        dataSets.add(bvd);
        dataSets.add(cnr);
        dataSets.add(registroImprese);
        dataSets.add(patiris);
        dataSets.add(scanr);
        dataSets.add(p3);
        dataSets.add(grid);
        dataSets.add(fwf);
        dataSets.add(sicris);


        Set<String> ids = new HashSet<>();


        Map<String, Record> idRecord = new HashMap<>();


        for (DataSet<Record, Attribute> dataset : dataSets) {
            for (Record r : dataset.get()) {

                String id = r.getValue(Organization.ID);
                ids.add(id);


                idRecord.put(id, r);


            }
        }


        String correspondenceFile = "/Users/paolosottovia/Downloads/correspondenceOrganizations_labelQgrams_standard.tsv";

        Set<Set<String>> correspondences = readCorrespondences(correspondenceFile);

        String correspondenceFileDistance = "/Users/paolosottovia/Downloads/correspondenceOrganizations_labelQgrams_DISTANCE.tsv";

        Set<Set<String>> correspondencesDistance = readCorrespondences(correspondenceFileDistance);


        String correspondenceFileNOClustering = "/Users/paolosottovia/Downloads/correspondenceOrganizations_labelQgrams_NO_CLUSTERING.tsv";

        Set<Set<String>> correspondencesNoClustering = readCorrespondences(correspondenceFileNOClustering);
        Set<String> idsMATCHES = new HashSet<>();


        //read matches


        for (Set<String> corr : correspondences) {

            if (corr.size() > 1) {

                idsMATCHES.addAll(corr);


            }


        }

        //select 500 entities from matches


        Set<String> idNOTMATCH = new HashSet<>();


        for (String id : idRecord.keySet()) {
            if (!idsMATCHES.contains(id)) {
                idNOTMATCH.add(id);
            }
        }


        System.out.println("Number of entity that are into matches: " + idsMATCHES.size());
        System.out.println("Number of entity that are NOT into matches: " + idNOTMATCH.size());


        int k = 50;


        List<String> matches = new ArrayList<>();
        matches.addAll(idsMATCHES);

        List<String> no_matches = new ArrayList<>();
        no_matches.addAll(idNOTMATCH);


        List<String> id_yes = new ArrayList<>();
        List<String> id_no = new ArrayList<>();

        if (GENERATE_SAMPLE) {

            for (int i = 0; i < k; i++) {


                //yes cases

                Collections.shuffle(matches);

                id_yes.add(matches.get(0));


                Collections.shuffle(no_matches);

                id_no.add(no_matches.get(0));


            }


            writeMatches(id_yes, "yes_matches.txt");

            writeMatches(id_no, "no_matches.txt");


        } else {

            id_yes.addAll(readMatches("yes_matches.txt"));
            id_no.addAll(readMatches("no_matches.txt"));
        }


        // sel

        System.out.println("Matching examples: ");
        for (int i = 0; i < k; i++) {

            String id = id_yes.get(i);
            extractOrganizationIDs(id, correspondences, correspondencesDistance, correspondencesNoClustering, idRecord);
        }


        System.out.println("NOT Matching examples: ");
        for (int i = 0; i < k; i++) {

            String id = id_no.get(i);
            extractOrganizationIDs(id, correspondences, correspondencesDistance, correspondencesNoClustering, idRecord);
        }


        System.out.println("\n\nIDS: ");
        for (int i = 0; i < k; i++) {

            String id = id_yes.get(i);
            System.out.println(id);
        }


        System.out.println("NOT IDS ");
        for (int i = 0; i < k; i++) {

            String id = id_no.get(i);
            System.out.println(id);
        }


    }

    public static void writeMatches(List<String> matches, String filename) {
        BufferedWriter writer = null;

//        System.out.println("Number of edges: " + edges.size());
        try {
            writer = new BufferedWriter(new FileWriter(filename));

            for (String match : matches) {


                writer.write(match + "\n");


            }


            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<String> readMatches(String filename) {
        List<String> correspondes = new ArrayList<>();
        File fileDir = new File(filename);

        try {

            BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fileDir), "UTF8"));

            String str;
            int lineCount = 0;
            while ((str = in.readLine()) != null) {

                if (!str.trim().equals("")) {
                    correspondes.add(str);

                }
                lineCount++;
                // System.out.println(str);
            }

            in.close();
            System.out.println("Number of lines: " + lineCount);
        } catch (UnsupportedEncodingException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return correspondes;


    }


    public static void extractOrganizationIDs(String id, Set<Set<String>> matches, Set<Set<String>> matchesDistances, Set<Set<String>> matchesNOClustering, Map<String, Record> idRecord) {

        System.out.println("-------------------------------------------------------------");

        System.out.println("ID: " + id);
        Record r1 = idRecord.get(id);
        if (r1 != null) {
            System.out.println(r1.getValue(Organization.ID) + "\t" + r1.getValue(Organization.LABEL) + "\t" + r1.getValue(Organization.ADDRESS) + "\t" + r1.getValue(Organization.CITY) + "\t" + r1.getValue(Organization.LINK) + "\t");
        }
        System.out.println("\n------------NORMAL--------------\n");
        for (Set<String> corr : matches) {
            if (corr.contains(id)) {

                for (String idCorr : corr) {

                    Record r = idRecord.get(idCorr);
                    if (r != null) {
                        System.out.println(r.getValue(Organization.ID) + "\t" + r.getValue(Organization.LABEL) + "\t" + r.getValue(Organization.ADDRESS) + "\t" + r.getValue(Organization.CITY) + "\t" + r.getValue(Organization.LINK) + "\t");
                    } else {
                        System.out.println("NULL RECORD!!!");
                    }

                }

            }
        }

        System.out.println("\n------------DISTANCES--------------\n");

        for (Set<String> corr : matchesDistances) {
            if (corr.contains(id)) {

                for (String idCorr : corr) {

                    Record r = idRecord.get(idCorr);
                    if (r != null) {
                        System.out.println(r.getValue(Organization.ID) + "\t" + r.getValue(Organization.LABEL) + "\t" + r.getValue(Organization.ADDRESS) + "\t" + r.getValue(Organization.CITY) + "\t" + r.getValue(Organization.LINK) + "\t");
                    } else {
                        System.out.println("NULL RECORD!!!");
                    }

                }

            }
        }


        System.out.println("\n------------NOCLUSTERING--------------\n");

        for (Set<String> corr : matchesNOClustering) {
            if (corr.contains(id)) {

                for (String idCorr : corr) {

                    Record r = idRecord.get(idCorr);
                    if (r != null) {
                        System.out.println(r.getValue(Organization.ID) + "\t" + r.getValue(Organization.LABEL) + "\t" + r.getValue(Organization.ADDRESS) + "\t" + r.getValue(Organization.CITY) + "\t" + r.getValue(Organization.LINK) + "\t");
                    } else {
                        System.out.println("NULL RECORD!!!");
                    }

                }

            }
        }

        System.out.println("-------------------------------------------------------------\n");

    }


    public static Set<Set<String>> readCorrespondences(String path) {

        Set<Set<String>> correspondes = new HashSet<>();

        File fileDir = new File(path);

        try {

            BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fileDir), "UTF8"));

            String str;
            int lineCount = 0;
            while ((str = in.readLine()) != null) {

                if (!str.trim().equals("")) {
                    String[] items = str.split("\t");
                    Set<String> corr = new HashSet<>();
                    for (String item : items) {
                        corr.add(item);
                    }
                    if (correspondes.contains(corr)) {
                        System.out.println("Duplicate: " + corr.toString());
                    }
                    if (corr.size() != 1) {

                        correspondes.add(corr);
                    }

                }
                lineCount++;
                // System.out.println(str);
            }

            in.close();
            System.out.println("Number of lines: " + lineCount);
        } catch (UnsupportedEncodingException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return correspondes;

    }
}
