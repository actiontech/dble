package io.mycat.plan.common.typelib;

public class TypeLib {
    public int count; /* How many types */
    public String name; /* Name of typelib */
    public String[] typeNames;
    public Integer typeLengths;

    public TypeLib(int countPar, String namePar, String[] typeNamesPar, Integer typeLengthsPar) {
        this.count = countPar;
        this.name = namePar;
        this.typeNames = typeNamesPar;
        this.typeLengths = typeLengthsPar;
    }
}
