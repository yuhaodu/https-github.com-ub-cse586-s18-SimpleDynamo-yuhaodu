package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by du on 04/04/2018.
 */

public class Message implements Serializable {
    public String className;
    public  String sendPort;
    public String selfPort;
    public String location;
    public String key;
    public String value;
    public String start;
    public String middle;
    public String end;
    public String missPort;
    public ConcurrentHashMap<String,String> map;
    public void ini_start(String start){this.start = start;}
    public void ini_middle(String middle){this.middle = middle;}
    public void ini_end(String end){this.end = end;}
    public void ini_selfPort(String selfPort){
        this.selfPort = selfPort;
    }
    public void ini_sendPort(String sendPort){
        this.sendPort = sendPort;
    }
    public void ini_className(String className){
        this.className = className;
    }
    public void ini_Location(String location){
        this.location = location;
    }
    public void ini_Key(String key){
        this.key = key;
    }
    public void ini_missPort(String missPort){this.missPort = missPort;}
    public void ini_Value(String value){
        this.value = value;
    }
    public void ini_HashMap(ConcurrentHashMap map){this.map= map;}
    public void fi_location(String sendPort,String start, String middle, String end, ConcurrentHashMap map){
        ini_sendPort(sendPort);
        ini_start(start);
        ini_middle(middle);
        ini_end(end);
        ini_HashMap(map);
        ini_className("fi_location");
    }
    public void re_InsertMessage(String sendPort,String keyfile, String valuefile){
        ini_Key(keyfile);
        ini_Value(valuefile);
        ini_sendPort(sendPort);
        ini_className("re_InsertMessage");
    }
    public void re_QueryMessage(String sendPort,String keyfile,String selfPort){
        ini_Key(keyfile);
        ini_sendPort(sendPort);
        ini_selfPort(selfPort);
        ini_className("re_QueryMessage");
    }
    public void reply_QueryMessage(String sendPort,String keyfile,String valuefile){
        ini_Key(keyfile);
        ini_Value(valuefile);
        ini_sendPort(sendPort);
        ini_className("reply_QueryMessage");
    }
    public void re_join(String sendPort,String selfPort,String location){
        ini_sendPort(sendPort);
        ini_selfPort(selfPort);
        ini_Location(location);
        ini_className("re_join");
    }
    public void re_QueryALL(String sendPort,String selfPort, ConcurrentHashMap<String,String> map){
        ini_sendPort(sendPort);
        ini_selfPort(selfPort);
        ini_HashMap(map);
        ini_className("re_QueryALL");
    }
    public void re_delete(String sendPort,String selfPort ,String key){
        ini_sendPort(sendPort);
        ini_Key(key);
        ini_selfPort(selfPort);
        ini_className("re_delete");
    }
    public void re_deleteAll(String sendPort,String selfPort){
        ini_sendPort(sendPort);
        ini_selfPort(selfPort);
        ini_className("re_deleteAll");
    }
    public void ini_hello(String sendPort,String selfPort){
        ini_sendPort(sendPort);
        ini_selfPort(selfPort);
        ini_className("ini_hello");
    }
    public void re_hello(String sendPort,ConcurrentHashMap<String,String> map){
        ini_className("re_hello");
        ini_sendPort(sendPort);
        ini_HashMap(map);
    }
    public void re_missReport(String sendPort,String missPort){
        ini_sendPort(sendPort);
        ini_missPort(missPort);
        ini_className("re_missReport");
    }

}