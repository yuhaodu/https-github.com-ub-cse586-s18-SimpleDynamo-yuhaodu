package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;

import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.TAG;

/**
 * Created by du on 2018/4/20.
 */

public class Circle {
    private String[] sequence = {"11124","11112","11108","11116","11120"};
    //private HashMap<String,String> endPort = new HashMap<String,String>();
    private String missPort = "no";
    public ArrayList<String> getWriteList(String key){
        String iniPort = ini_Port(key);
        int loc = checkS(iniPort);
        int loc1 = loc + 1;
        int loc2 = loc + 2;
        if(loc1 >= 5){
            loc1 = loc1 - 5;
        }
        if(loc2 >= 5){
            loc2 = loc2 -5;
        }
        ArrayList<String> result = new ArrayList<String>();
        result.add(sequence[loc]);
        result.add(sequence[loc1]);
        result.add(sequence[loc2]);
        int size = result.size();
        for(int i = 0 ; i<size ; i++){
            if(result.get(i).equals(missPort)){
                result.remove(i);
                break;
            }
        }
        return result;
    }
    public String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    public String getLocation(String input){
        try{
            String location = genHash(input);
            return location;
        }catch(NoSuchAlgorithmException e){
            Log.e(TAG,"NoSuchAlgorithmException at getLocation:"+input);
            return "wrong here";
        }

    }
    public String getPortLocation(String port){
        int portnum = Integer.parseInt(port);
        String location = getLocation(String.valueOf(portnum/2));
        return location;
    }
    public String ini_Port(String key){
        if(getLocation(key).compareTo(getPortLocation("11124"))>0 && getLocation(key).compareTo(getPortLocation("11112"))<0){
            return "11112";
        }
        else if(getLocation(key).compareTo(getPortLocation("11112"))>0 && getLocation(key).compareTo(getPortLocation("11108"))<0){
            return "11108";
        }
        else if(getLocation(key).compareTo(getPortLocation("11108"))>0 && getLocation(key).compareTo(getPortLocation("11116"))<0){
            return "11116";
        }
        else if(getLocation(key).compareTo(getPortLocation("11116"))>0 && getLocation(key).compareTo(getPortLocation("11120"))<0){
            return "11120";
        }
        else{
            return "11124";
        }

    }
    public String getSuccess(String myPort){
        int i = checkS(myPort);
        i = i + 1;
        if(sequence[getTruelocation(i)].equals(missPort)){
            return sequence[getTruelocation(i+1)];
        }else{
            return sequence[getTruelocation(i)];
        }
    }
    public void ini_missPort(String missPort){
        this.missPort = missPort;
    }
    public void rev_missPort(){
        missPort = "no";
    }
    public int checkS(String Port){
        int result = 9;
        for(int i =0; i<5; i++){
            if(sequence[i].equals(Port)){
                result = i;
                break;
            }
        }
        return result;
    }
    public int getTruelocation(int location){
        if(location >= 5){
            return location - 5;
        }
        else{
            return location;
        }
    }
    public String[] getSequence(){
        return sequence;
    }
    public String getMissport(){ return missPort;}
    public String[] threeBrother(String myPort){
        int location = checkS(myPort);
        if(location==0){
            String[] output = {"11116","11120","11112"};
            return output;
        }
        else if(location == 1){
            String[] output = {"11120","11124","11108"};
            return output;
        }
        else if(location == 2){
            String[] output = {"11124","11112","11116"};
            return output;
        }
        else if(location == 3){
            String[] output = {"11112","11108","11120"};
            return output;
        }
        else{
            String[] output = {"11108","11116","11124"};
            return output;
        }


    }


 }
