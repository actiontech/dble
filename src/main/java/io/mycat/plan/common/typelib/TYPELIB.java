package io.mycat.plan.common.typelib;

public class TYPELIB {
	public int count; /* How many types */
	public String name; /* Name of typelib */
	public String[] type_names;
	public Integer type_lengths;

	public TYPELIB(int count_par, String name_par, String[] type_names_par, Integer type_lengths_par) {
		this.count = count_par;
		this.name = name_par;
		this.type_names = type_names_par;
		this.type_lengths = type_lengths_par;
	}
}
