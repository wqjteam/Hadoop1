package com.wqj;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        HashMap map=new HashMap<String,String>();
        map.put(null,null);
        Hashtable table=new Hashtable<String,String>();
        //报错
        table.put("1",null);

        List list=null;
        Set set=null;
        System.out.println("11111111111");
        table.put(null,null);









        System.out.println( "Hello World!" );
    }
}
