package com.penspark.inits.xml;

public class Utils {

public static int getItsInt(String input){
    try{
    return Integer.parseInt(input);
    }catch(NumberFormatException e){
        
    }
    return 0;
}
}
