package it.unimore.deduplication.model;

import de.uni_mannheim.informatik.dws.winter.model.AbstractRecord;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Project extends AbstractRecord<Attribute> implements Serializable {

    public static final Attribute ACRONYM = new Attribute("acronym");
    public static final Attribute LABEL = new Attribute("label");
    public static final Attribute CALL = new Attribute("city");
    public static final Attribute DESCRIPTION = new Attribute("description");
    public static final Attribute CALL_LABEL = new Attribute("postcode");
    //    public static final Attribute LINK = new Attribute("links");
    public static final Attribute START_DATE = new Attribute("startDate");
    public static final Attribute TYPE = new Attribute("type");
    public static final Attribute YEAR = new Attribute("year");

    public static final Attribute MONTH = new Attribute("month");
    public static final Attribute ORGS = new Attribute("orgs");

    public static String id;
    public static String acronym;
    public static String label;
    public static String description;
    public static String duration;
    public static String budget;
    public static String call;
    public static String callLabel;
    public static String url;
    public static String startDate;
    public static String type;
    public static String year;
    public static String month;
    public static String themes;
    public static String orgs;


    public static Attribute getACRONYM() {
        return ACRONYM;
    }

    public static Attribute getLABEL() {
        return LABEL;
    }

    public static Attribute getCALL() {
        return CALL;
    }

    public static Attribute getCallLabel() {
        return CALL_LABEL;
    }

    public static void setCallLabel(String callLabel) {
        Project.callLabel = callLabel;
    }

    public static String getUrl() {
        return url;
    }

    public static void setUrl(String url) {
        Project.url = url;
    }


    public static Attribute getStartDate() {
        return START_DATE;
    }

    public static void setStartDate(String startDate) {
        Project.startDate = startDate;
    }

    public static String getType() {
        return type;
    }

    public static void setType(String type) {
        Project.type = type;
    }

    public static String getYear() {
        return year;
    }

    public static void setYear(String year) {
        Project.year = year;
    }

    public static String getMonth() {
        return month;
    }

    public static void setMonth(String month) {
        Project.month = month;
    }

    public static String getThemes() {
        return themes;
    }

    public static void setThemes(String themes) {
        Project.themes = themes;
    }

    public static String getOrgs() {
        return orgs;
    }

    public static void setOrgs(String orgs) {
        Project.orgs = orgs;
    }

    public static Attribute getTYPE() {
        return TYPE;
    }

    public static Attribute getYEAR() {
        return YEAR;
    }

    public static Attribute getMONTH() {
        return MONTH;
    }

    public static Attribute getORGS() {
        return ORGS;
    }

    public static String getId() {
        return id;
    }

    public static void setId(String id) {
        Project.id = id;
    }

    public static String getAcronym() {
        return acronym;
    }

    public static void setAcronym(String acronym) {
        Project.acronym = acronym;
    }

    public static String getLabel() {
        return label;
    }

    public static void setLabel(String label) {
        Project.label = label;
    }

    public static String getDescription() {
        return description;
    }

    public static void setDescription(String description) {
        Project.description = description;
    }

    public static String getDuration() {
        return duration;
    }

    public static void setDuration(String duration) {
        Project.duration = duration;
    }

    public static String getBudget() {
        return budget;
    }

    public static void setBudget(String budget) {
        Project.budget = budget;
    }

    public static String getCall() {
        return call;
    }

    public static void setCall(String call) {
        Project.call = call;
    }


    public static List<Attribute> getRelevantAttributes() {
        List<Attribute> attributes = new ArrayList<>();

        attributes.add(LABEL);
        attributes.add(DESCRIPTION);
        attributes.add(CALL);
        attributes.add(START_DATE);
        attributes.add(ACRONYM);
        attributes.add(YEAR);
        attributes.add(CALL_LABEL);


        return attributes;
    }

    @Override
    public boolean hasValue(Attribute attribute) {


        if (attribute == LABEL)
            return getLabel() != null && !getLabel().isEmpty();
        else if (attribute == ACRONYM)
            return getAcronym() != null && !getAcronym().isEmpty();
        else if (attribute == DESCRIPTION)
            return getDescription() != null;
        else if (attribute == CALL)
            return getCall() != null;
        else if (attribute == START_DATE)
            return getStartDate() != null;
        else if (attribute == YEAR)
            return getYear() != null;
        else if (attribute == CALL_LABEL)
            return getCallLabel() != null;

        else
            return false;
    }
}
