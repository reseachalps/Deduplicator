package it.unimore.deduplication.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import de.uni_mannheim.informatik.dws.winter.model.AbstractRecord;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;

public class Organization extends AbstractRecord<Attribute> implements Serializable {

    public static final Attribute ID = new Attribute("id");
    public static final Attribute LABEL = new Attribute("label");
    public static final Attribute CITY = new Attribute("city");
    public static final Attribute ADDRESS = new Attribute("address");
    public static final Attribute POSTCODE = new Attribute("postcode");
    public static final Attribute LINK = new Attribute("links");
    public static final Attribute ACRONYM = new Attribute("acronym");
    public static final Attribute CREATION_YEAR = new Attribute("creationYear");
    public static final Attribute URBAN_UNIT = new Attribute("urbanUnit");
    public static final Attribute COUNTRY_CODE = new Attribute("countryCode");

    public static final Attribute NUTS_1 = new Attribute("nuts1");
    public static final Attribute NUTS_2 = new Attribute("nuts2");
    public static final Attribute NUTS_3 = new Attribute("nuts3");


    public static final Attribute STAFF = new Attribute("staff");
    static String id;
    static String acronym;
    static String alias;
    static String label;
    static String creationYear;
    static String commercialLabel;
    static String address;
    static String city;
    static String citycode;
    static String country;
    static String countryCode;
    static String postcode;
    static String urbanUnit;
    static String urbanUnitCode;
    static String lat;
    static String lon;
    static String revenueRange;
    static String privateFinanceDate;
    static String employees;
    static String typeCategoryCode;
    static String typeLabel;
    static String typeKind;
    static String isPublic;
    static String leaders;
    static String links;
    static String privateOrgTypeId;
    static String privateOrgTypeLabel;
    static String activities;
    static String relations;
    static String badges;
    static String children;

    static String nuts1;
    static String nuts2;
    static String nuts3;

    static String staff;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAcronym() {
        return acronym;
    }

    public void setAcronym(String acronym) {
        this.acronym = acronym;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getCreationYear() {
        return creationYear;
    }

    public void setCreationYear(String creationYear) {
        this.creationYear = creationYear;
    }

    public String getCommercialLabel() {
        return commercialLabel;
    }

    public void setCommercialLabel(String commercialLabel) {
        this.commercialLabel = commercialLabel;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCitycode() {
        return citycode;
    }

    public void setCitycode(String citycode) {
        this.citycode = citycode;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public String getPostcode() {
        return postcode;
    }

    public void setPostcode(String postcode) {
        this.postcode = postcode;
    }

    public String getUrbanUnit() {
        return urbanUnit;
    }

    public void setUrbanUnit(String urbanUnit) {
        this.urbanUnit = urbanUnit;
    }

    public String getUrbanUnitCode() {
        return urbanUnitCode;
    }

    public void setUrbanUnitCode(String urbanUnitCode) {
        this.urbanUnitCode = urbanUnitCode;
    }

    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public String getLon() {
        return lon;
    }

    public void setLon(String lon) {
        this.lon = lon;
    }

    public String getRevenueRange() {
        return revenueRange;
    }

    public void setRevenueRange(String revenueRange) {
        this.revenueRange = revenueRange;
    }

    public String getPrivateFinanceDate() {
        return privateFinanceDate;
    }

    public void setPrivateFinanceDate(String privateFinanceDate) {
        this.privateFinanceDate = privateFinanceDate;
    }

    public String getEmployees() {
        return employees;
    }

    public void setEmployees(String employees) {
        this.employees = employees;
    }

    public String getTypeCategoryCode() {
        return typeCategoryCode;
    }

    public void setTypeCategoryCode(String typeCategoryCode) {
        this.typeCategoryCode = typeCategoryCode;
    }

    public String getTypeLabel() {
        return typeLabel;
    }

    public void setTypeLabel(String typeLabel) {
        this.typeLabel = typeLabel;
    }

    public String getTypeKind() {
        return typeKind;
    }

    public void setTypeKind(String typeKind) {
        this.typeKind = typeKind;
    }

    public String getIsPublic() {
        return isPublic;
    }

    public void setIsPublic(String isPublic) {
        this.isPublic = isPublic;
    }

    public String getLeaders() {
        return leaders;
    }

    public void setLeaders(String leaders) {
        this.leaders = leaders;
    }

    public String getLinks() {
        return links;
    }

    public void setLinks(String links) {
        this.links = links;
    }

    public String getPrivateOrgTypeId() {
        return privateOrgTypeId;
    }

    public void setPrivateOrgTypeId(String privateOrgTypeId) {
        this.privateOrgTypeId = privateOrgTypeId;
    }

    public String getPrivateOrgTypeLabel() {
        return privateOrgTypeLabel;
    }

    public void setPrivateOrgTypeLabel(String privateOrgTypeLabel) {
        this.privateOrgTypeLabel = privateOrgTypeLabel;
    }

    public String getActivities() {
        return activities;
    }

    public void setActivities(String activities) {
        this.activities = activities;
    }

    public String getRelations() {
        return relations;
    }

    public void setRelations(String relations) {
        this.relations = relations;
    }

    public String getBadges() {
        return badges;
    }

    public void setBadges(String badges) {
        this.badges = badges;
    }

    public String getChildren() {
        return children;
    }

    public void setChildren(String children) {
        this.children = children;
    }


    public static String getStaff() {
        return staff;
    }

    public static void setStaff(String staff) {
        Organization.staff = staff;
    }


    public static String getNuts1() {
        return nuts1;
    }

    public static void setNuts1(String nuts1) {
        Organization.nuts1 = nuts1;
    }


    public static String getNuts2() {
        return nuts2;
    }

    public static void setNuts2(String nuts2) {
        Organization.nuts2 = nuts2;
    }


    public static String getNuts3() {
        return nuts3;
    }

    public static void setNuts3(String nuts3) {
        Organization.nuts3 = nuts3;
    }


    public static List<Attribute> getRelevantAttributes() {
        List<Attribute> attributes = new ArrayList<>();

        attributes.add(LABEL);
        attributes.add(CITY);
        attributes.add(POSTCODE);
        attributes.add(LINK);
        attributes.add(ACRONYM);
        attributes.add(CREATION_YEAR);
        attributes.add(URBAN_UNIT);
        attributes.add(COUNTRY_CODE);
        attributes.add(STAFF);
        attributes.add(NUTS_1);
        attributes.add(NUTS_2);
        attributes.add(NUTS_3);

        return attributes;
    }


    @Override
    public boolean hasValue(Attribute attribute) {
        if (attribute == LABEL)
            return getLabel() != null && !getLabel().isEmpty();
        else if (attribute == CITY)
            return getCity() != null && !getCity().isEmpty();
        else if (attribute == POSTCODE)
            return getPostcode() != null;
        else if (attribute == LINK)
            return getLinks() != null;
        else if (attribute == ACRONYM)
            return getAcronym() != null;
        else if (attribute == CREATION_YEAR)
            return getCreationYear() != null;
        else if (attribute == URBAN_UNIT)
            return getUrbanUnit() != null;
        else if (attribute == COUNTRY_CODE)
            return getCountryCode() != null;
        else if (attribute == STAFF)
            return getStaff() != null;
        else if (attribute == NUTS_1)
            return getNuts1() != null;
        else if (attribute == NUTS_2)
            return getNuts2() != null;
        else if (attribute == NUTS_3)
            return getNuts3() != null;


        return false;

    }


}
