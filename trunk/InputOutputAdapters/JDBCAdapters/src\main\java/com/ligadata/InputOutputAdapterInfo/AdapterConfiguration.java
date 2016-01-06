package com.ligadata.InputOutputAdapterInfo;

public abstract class AdapterConfiguration {
	  private java.lang.String Name;
	  private java.lang.String formatOrInputAdapterName;
	  private java.lang.String associatedMsg;
	  private java.lang.String className;
	  private java.lang.String jarName;
	  private scala.collection.immutable.Set<java.lang.String> dependencyJars;
	  private java.lang.String adapterSpecificCfg;
	  private java.lang.String keyAndValueDelimiter;
	  private java.lang.String fieldDelimiter;
	  private java.lang.String valueDelimiter;
	  
	  public abstract java.lang.String Name();
	  public abstract void Name_$eq(java.lang.String string);
	  public abstract java.lang.String formatOrInputAdapterName();
	  public abstract void formatOrInputAdapterName_$eq(java.lang.String string);
	  public abstract java.lang.String associatedMsg();
	  public abstract void associatedMsg_$eq(java.lang.String string);
	  public abstract java.lang.String className();
	  public abstract void className_$eq(java.lang.String string);
	  public abstract java.lang.String jarName();
	  public abstract void jarName_$eq(java.lang.String string);
	  public abstract scala.collection.immutable.Set<java.lang.String> dependencyJars();
	  public abstract void dependencyJars_$eq(scala.collection.immutable.Set<java.lang.String> set);
	  public abstract java.lang.String adapterSpecificCfg();
	  public abstract void adapterSpecificCfg_$eq(java.lang.String string);
	  public abstract java.lang.String keyAndValueDelimiter();
	  public abstract void keyAndValueDelimiter_$eq(java.lang.String string);
	  public abstract java.lang.String fieldDelimiter();
	  public abstract void fieldDelimiter_$eq(java.lang.String string);
	  public abstract java.lang.String valueDelimiter();
	  public abstract void valueDelimiter_$eq(java.lang.String string);
	  
}
