package edu.ucr.cs242;

public class Subroutine {
    private String name;
    private String className;
    private String description;

    public String getName() {
        return name;
    }

    public String getClassName() {
        return className;
    }

    public String getDescription() {
        return description;
    }

    public Subroutine(String name, String className, String description) {
        this.name = name;
        this.className = className;
        this.description = description;
    }
}