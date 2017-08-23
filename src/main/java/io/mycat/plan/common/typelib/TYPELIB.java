package io.mycat.plan.common.typelib;

public class TYPELIB {
    public int count; /* How many types */
    public String name; /* Name of typelib */
    public String[] typeNames;
    public Integer typeLengths;

    public TYPELIB(int countPar, String namePar, String[] typeNamesPar, Integer typeLengthsPar) {
        this.count = countPar;
        this.name = namePar;
        this.typeNames = typeNamesPar;
        this.typeLengths = typeLengthsPar;
    }
}
