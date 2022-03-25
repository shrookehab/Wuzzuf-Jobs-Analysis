package com.team3.WuzzafJobs;

import java.util.Comparator;

public class JobPOJO implements Comparator<JobPOJO> {
    private String title;
    private String company;
    private String location;
    private String type;
    private String level;
    private String yearsExp;
    private String country;
    private String skills;

    public JobPOJO(String title, String company, String location, String type, String level, String yearsExp, String country, String skills) {
        this.title = title;
        this.company = company;
        this.location = location;
        this.type = type;
        this.level = level;
        this.yearsExp = yearsExp;
        this.country = country;
        this.skills = skills;
    }

    @Override
    public String toString() {
        return "JobPOJO{" +
                "title='" + title + '\'' +
                ", company='" + company + '\'' +
                ", location='" + location + '\'' +
                ", type='" + type + '\'' +
                ", level='" + level + '\'' +
                ", yearsExp='" + yearsExp + '\'' +
                ", country='" + country + '\'' +
                ", skills='" + skills + '\'' +
                '}';
    }



    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getYearsExp() {
        return yearsExp;
    }

    public void setYearsExp(String yearsExp) {
        this.yearsExp = yearsExp;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getSkills() {
        return skills;
    }

    public void setSkills(String skills) {
        this.skills = skills;
    }

    @Override
    public int compare(JobPOJO job1, JobPOJO job2) {
        return job1.getCountry().compareTo(job2.getCountry()) & job1.getCompany().compareTo(job2.getCompany()) &
                job1.getYearsExp().compareTo(job2.getYearsExp()) & job1.getLevel().compareTo(job2.getLevel()) &
                job1.getLocation().compareTo(job2.getLocation()) & job1.getTitle().compareTo(job2.getTitle()) &
                job1.getType().compareTo(job2.getType()) & job1.getSkills().compareTo(job2.getSkills()) ;
    }
}
