package com.brunoabreu;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DesafioSemantix {

    private static final Pattern HOST = Pattern.compile("(.+) - - ");
    private static final Pattern TIMESTAMP = Pattern.compile("\\[\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}[ -]+\\d{4}\\]");
    private static final Pattern DATE = Pattern.compile(":");
    private static final Pattern REQUEST = Pattern.compile("\\\"(.*?)\\\"");
    private static final Pattern RESPONSE = Pattern.compile("\\\" ([0-9]+ [0-9]+)");
    private static final Pattern RESPONSECODE = Pattern.compile("\\\" ([0-9]+)");
    private static final Pattern SPC = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        exec("C:\\access_log_Jul95");
        exec("C:\\access_log_Aug95");
    }

    public static void exec(String file) throws Exception {
        System.out.println("\nExecutando arquivo: "+file);
        SparkSession spark = SparkSession
                .builder()
                .appName("DesafioSemantix")
                .config("spark.master", "local")
                .getOrCreate();
        JavaRDD<String> lines = spark.read().textFile(file).javaRDD();
        int total404 = 0;
        ArrayList<String> hostsList = new ArrayList<>();
        HashMap<String,Long> urlList = new HashMap<>();
        HashMap<String,Long> dateList = new HashMap<>();
        Long totalResponseSize = 0L;
        for(String line:lines.collect()) {
            String host;
            try {
                Matcher hostMatcher = HOST.matcher(line);
                hostMatcher.find();
                host = SPC.split(hostMatcher.group(0))[0];
            } catch (IllegalStateException e) {
                host = "/";
            }
            String timestamp;
            try {
                Matcher timestampMatcher = TIMESTAMP.matcher(line);
                timestampMatcher.find();
                timestamp = timestampMatcher.group(0);
            } catch (IllegalStateException e) {
                timestamp = "[00/XXX/0000:00:00:003 -0000]";
            }

            String date = DATE.split(timestamp)[0].substring(1);
            String request;
            try {
                Matcher requestMatcher = REQUEST.matcher(line);
                requestMatcher.find();
                request = requestMatcher.group(0);
            } catch (IllegalStateException e) {
                request = "null";
            }

            String url;
            try {
                url = host+SPC.split(request)[1];
            } catch (ArrayIndexOutOfBoundsException e) {
                url = "/";
            }

            Long responseCode;
            Long responseSize;
            try {
                Matcher responseMatcher = RESPONSE.matcher(line);
                responseMatcher.find();
                String response = responseMatcher.group(0);
                responseCode = Long.valueOf(SPC.split(response)[1]);
                responseSize = Long.valueOf(SPC.split(response)[2]);
            } catch (IllegalStateException e) {
                try {
                    Matcher responseMatcher = RESPONSECODE.matcher(line);
                    responseMatcher.find();
                    String response = responseMatcher.group(0);
                    responseCode = Long.valueOf(SPC.split(response)[1]);
                    responseSize = 0L;
                } catch (IllegalStateException e2) {
                    responseCode = 0L;
                    responseSize = 0L;
                }
            }
            if (responseCode==404) total404++;
            if (!hostsList.contains(host)) hostsList.add(host);
            if (urlList.containsKey(url)&&responseCode==404) {
                urlList.put(url,urlList.get(url)+1);
            } else if (responseCode==404) urlList.put(url,1L);
            if (dateList.containsKey(date)&&responseCode==404) {
                dateList.put(date,dateList.get(date)+1);
            } else if (responseCode==404) dateList.put(date,1L);
            totalResponseSize = totalResponseSize + responseSize;
        }
        System.out.println("1. Número de hosts únicos: "+hostsList.size());
        System.out.println("2. Total de erros 404: "+total404);
        System.out.println("3. Os 5 Urls que mais causaram erro 404: ");
        Object[] sortUrl = urlList.entrySet().toArray();
        Arrays.sort(sortUrl, new Comparator() {
            public int compare(Object url1, Object url2) {
                return ((Map.Entry<String,Long>) url2).getValue().compareTo(((Map.Entry<String,Long>) url1).getValue());
            }
        });
        int maxValue = 5;
        for (int i=1;i<=maxValue;i++) {
            String key = ((Map.Entry<String, Long>) sortUrl[i]).getKey();
            System.out.println(i + "º: " + key);
        }
        System.out.println("4. Quantidade de erros 404 por dia: ");
        Iterator it = dateList.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            System.out.println("Dia: "+pair.getKey() + " | Quantidade 404: " + pair.getValue());
            it.remove(); // avoids a ConcurrentModificationException
        }
        System.out.println("5. Total de bytes retornados: "+totalResponseSize);
        spark.stop();
    }
}
