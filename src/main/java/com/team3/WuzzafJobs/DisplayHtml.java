package com.team3.WuzzafJobs;

import org.apache.commons.codec.binary.Base64;
import org.apache.spark.sql.Row;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DisplayHtml {
    private static HTMLTableBuilder builder ;

    public static String displayrows(String []head, List<Row> ls){

        builder=new HTMLTableBuilder(null,true,3,head.length);
        builder.addTableHeader(head);
        for (Row r : ls) {
            String[] s = r.toString().replace("[","").replace("]","")
                    .split(",", head.length);
            builder.addRowValues(s);

        }
        return builder.build();


    }

    public static String displayStrings(String []head, String[] ls){

        builder=new HTMLTableBuilder(null,true,3,head.length);
        builder.addTableHeader(head);
        String[] s1 = {"", "", ""};
        String[] s = ls[2].replaceAll("-", "").split("\\|");
        for(int i = 0; i < s.length; i+=3){
            s1[0] = s[i];
            s1[1] = s[i + 1];
            s1[2] = s[i + 2];
            builder.addRowValues(s1);
        }
        return builder.build();


    }

    public static String displayMap(String []head, Map<String, Long> MapString){

        builder=new HTMLTableBuilder(null,true,3,head.length);
        builder.addTableHeader(head);
        String[] s1 = {"", ""};
        for (Map.Entry<String, Long> m : MapString.entrySet()) {
            s1[0] = m.getKey();
            s1[1] = m.getValue().toString();
            builder.addRowValues(s1);
        }
        return builder.build();


    }

    public static String viewchart(String path){

        FileInputStream img ;
        try {
            File f= new File(path);
            img = new FileInputStream(f);
            byte[] bytes =  new byte[(int)f.length()];
            img.read(bytes);
            String encodedfile = new String(Base64.encodeBase64(bytes) , "UTF-8");

            return "<div>" +
                    "<img src=\"data:image/png;base64, "+encodedfile+"\" alt=\"Red dot\" />" +
                    "</div>";
        } catch (IOException e) {
            return "error";
        }



    }
}
