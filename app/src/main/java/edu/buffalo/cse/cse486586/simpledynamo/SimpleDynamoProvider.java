package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.InputStream;
import java.io.StreamCorruptedException;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import static android.os.AsyncTask.SERIAL_EXECUTOR;


public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final String EMULATER0 = "5554";
	static final String EMULATER1 = "5556";
	static final String EMULATER2 = "5558";
	static final String EMULATER3 = "5560";
	static final String EMULATER4 = "5562";
	static final int SERVER_PORT = 10000;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private ConcurrentHashMap<String,String> StoredMessage = new ConcurrentHashMap<String,String>();
	private ArrayList<String> sequence = new ArrayList<String>();
	private String myPort;
	private String myEmulator;
	private String mylocation;
	private final Uri mUri=buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	private HashMap<String,String> endPort = new HashMap<String,String>();
    private Circle circle = new Circle();
	private manipulateCursor macursor = new manipulateCursor();
	private manipulateCursor macursor2 = new manipulateCursor();
	private String successor;
	private int count = 0;
	private int t = 0;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		File dir = getContext().getFilesDir();
		if(selection.equals("@")){
			for(String key : StoredMessage.keySet()){
				File file = new File(dir,key);
				file.delete();
			}
			StoredMessage = new ConcurrentHashMap<String,String>();
		}
		else if(selection.equals("*")){

			for(String key : StoredMessage.keySet()){
				File file = new File(dir,key);
				file.delete();
			}
			StoredMessage = new ConcurrentHashMap<String,String>();
			Message message = new Message();
			successor = circle.getSuccess(myPort);
			message.re_deleteAll(successor,myPort);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
		}
		else{
			ArrayList<String> writeList = circle.getWriteList(selection);
			Message message = new Message();
			message.re_delete(writeList.get(0),myPort,selection);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		Set<String> myset = values.keySet();
		int size = myset.size();
		String[] settostring = myset.toArray(new String[size]);
		String keyfile = values.getAsString(settostring[1]);
		String valuefile = values.getAsString(settostring[0]);
		String location = getLocation(keyfile);
		String firstPort = circle.firstPort(keyfile);
		Message message = new Message();
		message.re_InsertMessage(firstPort,keyfile,valuefile,myPort);
		new ClientTask().executeOnExecutor(SERIAL_EXECUTOR, message);
		Log.v(TAG, "Function: Send Insert "+ keyfile +" request to " + firstPort );
		return uri;
	}

	@Override
	public boolean onCreate() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		myEmulator = portStr;
		mylocation = getLocation(myEmulator);
		createServerTask();
		String[] portList = circle.fourBrother(myPort);
		Message me = new Message();
		Message me1 = new Message();
		Message me2 = new Message();
		Message me3 = new Message();
		me.ini_hello(portList[0],myPort);
		new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,me);
		me1.ini_hello(portList[1],myPort);
		new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,me1);
		me2.ini_hello(portList[2],myPort);
		new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,me2);
		me3.ini_hello(portList[3],myPort);
		new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,me3);


		return true;


		// TODO Auto-generated method stub
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		FileInputStream fileInputStream;
		String value;
		String[] columnNames = new String[]{"key","value"};
		MatrixCursor matrixCursor = new MatrixCursor(columnNames);
		if(selection.equals("@")){
			try{
				for(String key : StoredMessage.keySet()){
					fileInputStream = getContext().openFileInput(key);
					BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
					value = reader.readLine();
					fileInputStream.close();
					matrixCursor.addRow(new Object[]{key,value});
					Log.v("@query",key + ";;" + value );
				}}catch(IOException e){
				Log.e(TAG,"query @ has problem");
			}
			return matrixCursor;
		}
		else if(selection.equals("*")){

			Log.v(TAG,"* QueryAll start ");
			Message message = new Message();
			successor = circle.getSuccess(myPort);
			message.re_QueryALL(successor,myPort,StoredMessage);
			new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,message);
			synchronized(macursor2){
				try{
					while(macursor2.matrixCursor == null){
						macursor2.wait();
					}
					Log.d(TAG,"receive all query");
					matrixCursor = macursor2.re_Cursor();
					macursor2.ini_Cursor(null);
				}catch(InterruptedException e){
					Log.e("TAG","InterruptedException");
				}
			}
			return matrixCursor;
		}
		else{
			/*try {
				fileInputStream = getContext().openFileInput(selection);
				BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
				value = reader.readLine();
				fileInputStream.close();
				matrixCursor.addRow(new Object[]{selection,value});
				Log.v("localquery", selection + ";;" + value);
			} catch (FileNotFoundException e) {*/
			String queryPort = circle.lastPort(selection);
			Message message = new Message();
			message.re_QueryMessage(queryPort,selection,myPort);
			Log.v(TAG,"Function: send query: "+message.key+" to "+ queryPort );
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
			synchronized (macursor){
				try{
						while(macursor.matrixCursor == null){
							macursor.wait();
						}
						Log.d(TAG,"found in other ContentProvider: " + matrixCursor.toString() );
						matrixCursor = macursor.re_Cursor();
						macursor.ini_Cursor(null);
				}catch(InterruptedException f){
						Log.e(TAG,"INterrputedException + Query");
						f.printStackTrace();
					}
				}
			return matrixCursor;}

		}

	private class ClientTask extends AsyncTask<Message, Void, Void> {
		protected Void doInBackground(Message... messages) {
			Message message = messages[0];
			String sendPort = message.sendPort;
			String name = message.className;
			try{
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),Integer.parseInt(sendPort));
			try{
				Log.d(TAG,"ClientTask: Send +"+message.className + " to " + message.sendPort );
				ObjectOutputStream output  = new ObjectOutputStream(socket.getOutputStream());
				output.writeObject(message);
				output.flush();
			}
			catch(UnknownHostException e){
				Log.e(TAG,"UnknownHostException");
			}
			catch(IOException e){
				Log.e(TAG,"ClientTask: Send IOException");
				IOExceptionHandle(message,e);
				return null;
			}
			try{
				if(message.className.equals("re_InsertMessage")){
					Log.e(TAG,"Wait for OK");
				    ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
				    Message message1 =(Message)input.readObject();
				    if(message1.className.equals("Fin_OK")){
						Log.d(TAG,"Receive Final Insert");
					}
				}
				else if(message.className.equals("re_QueryMessage")){
					Log.e(TAG,"Wait for Query" + message.key);
					ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
					Message message1 =(Message)input.readObject();
					if(message1.className.equals("reply_QueryMessage")){
						if(message1.value.equals("none") || message1.value==null){
							try {
								Thread.sleep(400);
								Log.e(TAG,"Wait 200ms for " + message1.key);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							Log.e(TAG,"ClientTask: Receive __NULL__ reply_QueryMessage for " + message1.key);
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
							Log.v(TAG,"ClientTask: Receive __NULL__ reply_QueryMessage and try second time" + message1.key);
							return null;
						}else{
						synchronized (macursor){
							Log.d(TAG,"reply query from other  other ContentProvider: key is: " + message1.key+" value is:" + message1.value);
							String[] columnNames = new String[]{"key","value"};
							MatrixCursor matrixCursor = new MatrixCursor(columnNames);
							matrixCursor.addRow(new Object[]{message1.key,message1.value});
							macursor.ini_Cursor(matrixCursor);
							macursor.notify();
						}}
					}
				}else if(name.equals("re_delete")){
					Log.e(TAG,"Wait for Delete");
					ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
					Message message1 =(Message)input.readObject();
					if(message1.className.equals("Fin_OK")){
						Log.d(TAG,"Receive Final Delete");

					}

				}
			}catch(UnknownHostException e){
				Log.e(TAG,"UnknownHostException");
			}catch(ClassNotFoundException e){
				Log.e(TAG,"ClassNotFoundException");
			} catch(IOException e){
				Log.e(TAG,"ClientTask: Received IOException");
				IOExceptionHandle(message,e);
				return null;
			}}
			catch(UnknownHostException e){
				Log.e(TAG,"UnknownHostException");
			}
			catch(IOException e){
				Log.e(TAG,"IOException");
			}

			return null;
		}
	}



	private class ServerTask extends AsyncTask<ServerSocket,String,Void>{
		protected Void doInBackground(ServerSocket... sockets){
			ServerSocket serverSocket = sockets[0];
			while(true) {
				try {
					Socket sockets1 = serverSocket.accept();
					ObjectInputStream input = new ObjectInputStream(sockets1.getInputStream());
					Message message =(Message)input.readObject();

					if(message.className.equals("re_InsertMessage")){          //re_InsertMessage
						String key = message.key;
						String value = message.value;
						fileInsert(key,value);
						Log.v(TAG,"ServerTask: Insert: " + key + "  " + value);
						String nextPort = circle.nextPort(message.key,myPort);
						if(nextPort.equals("no")){
							Log.v(TAG,"ServerTask: insert "+ key+"at:"+myPort+" end of insert");}
						else{
							message.ini_sendPort(nextPort);
							message.ini_selfPort(myPort);
							new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,message);
							Log.v(TAG,"ServerTask: Send resquest insert "+key+" to: "+nextPort);}
						Message message2 = new Message();
						ObjectOutputStream output = new ObjectOutputStream(sockets1.getOutputStream());
						message2.Fin_OK(message.selfPort);
						output.writeObject(message2);
						output.flush();

					}      /// Insert Message


					else if(message.className.equals("re_QueryMessage")){        //re_QueryMessage
						String value = "none";
						for( String key : StoredMessage.keySet()){
							if(key.equals(message.key)){
								value  = StoredMessage.get(key);
								break;
							}
						}
						Message message1 = new Message();
						message1.reply_QueryMessage(message.selfPort,message.key,value);
						ObjectOutputStream output = new ObjectOutputStream(sockets1.getOutputStream());
						output.writeObject(message1);
						output.flush();
						Log.d(TAG,"ServerTask: "+ "Found " + message.key + " send back to " + message.selfPort);

					}      ///  Query Message

					else if(message.className.equals("re_QueryALL")){
						if(!message.selfPort.equals(myPort)){
							Log.v("SeverTask received:","re_QueryALL " );
							ConcurrentHashMap<String, String> all = new ConcurrentHashMap<String,String>();
							all.putAll(message.map);
							all.putAll(StoredMessage);
							message.ini_HashMap(all);
							message.ini_sendPort(circle.getSuccess(myPort));
							new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,message);
						}else{
							synchronized (macursor2){
								Log.v(TAG,"received query all ");
								String[] columnNames = new String[]{"key","value"};
								MatrixCursor matrixCursor = new MatrixCursor(columnNames);
								ConcurrentHashMap<String,String> map = message.map;
								for(String key : message.map.keySet()){
									matrixCursor.addRow(new Object[]{key,map.get(key)});
								}
								macursor2.ini_Cursor(matrixCursor);
								macursor2.notify();
							}
						}
					}      /// Re_Query_All

					else if(message.className.equals("re_deleteAll")){
						String selfPort = message.selfPort;
						if(selfPort.equals(myPort)){
						}
						else{
							File dir = getContext().getFilesDir();
							for(String key : StoredMessage.keySet()){
								File file = new File(dir,key);
								file.delete();
							}
							StoredMessage = new ConcurrentHashMap<String, String>();
							message.ini_sendPort(circle.getSuccess(myPort));
							new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,message);
						}
					}

					else if(message.className.equals("re_delete")){
						Message message2 = new Message();
						ObjectOutputStream output = new ObjectOutputStream(sockets1.getOutputStream());
						message2.Fin_OK(message.selfPort);
						output.writeObject(message2);
						output.flush();
						String key = message.key;
						Log.d(TAG,"delete: " + key+ " at " + myPort );
						StoredMessage.remove(key);
						ArrayList<String> writeList = circle.getWriteList(key);
						int location = getSequenceNumber(writeList,myPort);
						if(writeList.size()-1 == location){
							Log.v("delete", key+"at:"+myPort+" end of insert");}
						else{
							message.ini_sendPort(writeList.get(location + 1));
							message.ini_selfPort(myPort);
							new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,message);
							Log.v("Send resquest delete", key+"to:"+writeList.get(location + 1));}

					}
					else if(message.className.equals("ini_hello")){
						Log.e(TAG,"ServerTask: Receive hello from" + message.selfPort);
						circle.rev_missPort();
						ConcurrentHashMap<String,String> sendMap = new ConcurrentHashMap<String,String>();
						String mPort = message.selfPort;
						String[] four = circle.fourBrother(mPort);
						for(int i = 0; i < 3; i++){
							if(myPort.equals(four[0]) || myPort.equals(four[1])){
								for(String key:StoredMessage.keySet()){
									if(checkbelongto(key).equals(myPort)){
										sendMap.put(key,StoredMessage.get(key));

									}
								}
								break;
							}
							else if(myPort.equals(four[2]) || myPort.equals(four[3])){
								for(String key:StoredMessage.keySet()){
									if(checkbelongto(key).equals(mPort)){
										sendMap.put(key,StoredMessage.get(key));
									}
								}
								break;

							}else{

							}
						}
						if(!sendMap.isEmpty()){
							Message message1 = new Message();
							message1.re_hello(mPort,myPort,sendMap);
							new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,message1);
							Log.e(TAG,"ServerTask: Send re_hello to" + mPort );
						}else{
							Message message1 = new Message();
							message1.re_hello(mPort,myPort,sendMap);
							new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,message1);
							Log.e(TAG,"ServerTask: Send re_hello to" + mPort );
						}
					}

					else if(message.className.equals("re_hello")){
						//StoredMessage.putAll(message.map);
						Map<String,String> map = message.map;
						for(String key:map.keySet()){
							fileInsert(key,map.get(key));
							Log.d(TAG,"ServerTask: received key from "+message.selfPort + " For " + key + " value is " + map.get(key));
						}
						Log.v(TAG,"SeverTask: re_hello from: " + message.selfPort );
						count = count + 1;
						if(count == 4 && t == 0){
							String[] portList = circle.fourBrother(myPort);
							Message mee = new Message();
							Message mee1 = new Message();
							Message mee2 = new Message();
							Message mee3 = new Message();
							mee.ini_hello(portList[0],myPort);
							new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,mee);
							mee1.ini_hello(portList[1],myPort);
							new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,mee1);
							mee2.ini_hello(portList[2],myPort);
							new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,mee2);
							mee3.ini_hello(portList[3],myPort);
							new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,mee3);
							count = 0;
							t = t+1;
						}
					}


				}catch(IOException e){
					Log.e(TAG,"ServerTask Exception");
					e.printStackTrace();
				}
				catch(ClassNotFoundException e){
					Log.e(TAG,"classNotFoundException" );
					e.printStackTrace();
				}

			}
		}
	}
	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
	private class manipulateCursor{
		MatrixCursor matrixCursor = null;
		public void ini_Cursor(MatrixCursor cursor){
			matrixCursor = cursor;
		}
		public MatrixCursor re_Cursor(){
			return matrixCursor;
		}

	}

	public void ini_Sequence(){
		sequence.add("11124");
		sequence.add("11112");
		sequence.add("11108");
		sequence.add("11116");
		sequence.add("11120");
		endPort.put("11124","11108");
		endPort.put("11112","11116");
		endPort.put("11108","11120");
		endPort.put("11116","11124");
		endPort.put("11124","11108");

	}
	public void change_Sequence(String missPort){
		int len = sequence.size();
		int index = 10;
		for(int i =0; i < len ; i++){
			if(missPort.equals(sequence.get(i))){
				index = i;
				break;
			}
		}
		sequence.remove(index);
	}
	public void add_Sequence(String onPort){
		if(onPort.equals("11124")){
			sequence.add(0,"11124");
		}
		else if(onPort.equals("11112")){
			sequence.add(1,"11112");
		}
		else if(onPort.equals("11108")){
			sequence.add(2,"11108");
		}
		else if(onPort.equals("11116")){
			sequence.add(3,"11116");
		}
		else{
			sequence.add(4,"11116");
		}


	}
	public String getPortLocation(String port){
		int portnum = Integer.parseInt(port);
		String location = getLocation(String.valueOf(portnum/2));
		return location;
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
	public void createServerTask(){
		try{

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,serverSocket);
		}catch (IOException e){
			Log.e(TAG,"Can't create a ServerSocket");
			e.printStackTrace();
		}
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
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	public void fileInsert(String keyfile, String valuefile){
		try {
			FileOutputStream outputStream = getContext().openFileOutput(keyfile, Context.MODE_PRIVATE);
			outputStream.write(valuefile.getBytes());
			outputStream.close();
			StoredMessage.put(keyfile,valuefile);
		} catch (Exception e) {
			Log.e(TAG, "File Output Wrong");

		}
	}
	public int getSequenceNumber(ArrayList<String> writeList,String myport) {
		int size = writeList.size();
		int output = 9;
		for (int i = 0; i < size; i++) {
			if (writeList.get(i).equals(myport)) {
				output = i;
				break;

			}

		}
		return output;

	}

	public String findNext(ArrayList<String> writeList, String nowPort){
		int index = 9;
		int size = writeList.size();
		for(int i =0; i < size; i++){
			if(nowPort.equals(writeList.get(i))){
				index = i;
				break;
			}
		}
		if(index == size-1){
			return "no";
		}
		else{
			return writeList.get(index+1);
		}

	}

	public void StreamExceptionHandle(Message message,Exception e){
		String sendPort = message.sendPort;
		Log.e(TAG,"StreamCorruptedException in ClientTask to "+sendPort+" ; "+message.className);
		String name = message.className;
		if(name.equals("re_InsertMessage")){
			ArrayList<String> trueList = circle.getTrueList(message.key);
			String next = findNext(trueList,sendPort);
			if(next.equals("no")){}
			else{
				message.ini_sendPort(next);
				new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,message);
				Log.v(TAG,"Client Task: Send resquest insert"+ message.key+"to:"+next+"After detect failure: "+sendPort);


			}
		}
		else if(name.equals("re_delete")){
			ArrayList<String> writeList = circle.getWriteList(message.key);
			String next = findNext(writeList,sendPort);
			if(next.equals("no")){}
			else{
				message.ini_sendPort(next);
				new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,message);
				Log.v("Send resquest delete", message.key+"to:"+next+"After detect failure: "+sendPort);
			}
		}
		circle.ini_missPort(sendPort);
		Log.e(TAG,"Missport-InI: " + sendPort);
		e.printStackTrace();
	}

	public void IOExceptionHandle(Message message,Exception e){
		String sendPort = message.sendPort;
		circle.ini_missPort(sendPort);
		Log.e(TAG,"Missport-InI: " + sendPort);
		Log.e(TAG,"IOException in ClientTask to "+sendPort+" ; "+message.className);
		String name = message.className;
		if(name.equals("re_InsertMessage")){
			String next = circle.nextPort(message.key,sendPort);
			if(next.equals("no")){}
			else{
				message.ini_sendPort(next);
				message.ini_selfPort(myPort);
				new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,message);
				Log.v(TAG,"Client Task: Send resquest insert"+ message.key+"to:"+next+"After detect failure: "+sendPort);
			}
		}
		else if(name.equals("re_QueryMessage")){
			String key = message.key;
			String[] writeList = circle.storeseq(key);
			String middlePort = writeList[1];
			message.ini_sendPort(middlePort);
			new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,message);
			Log.v(TAG,"Client Task: Send Query"+ message.key+"to: "+middlePort+" After detect failure: "+sendPort);
		}


		else if(name.equals("re_delete")){
			String next = circle.nextPort(message.key,sendPort);
			if(next.equals("no")){}
			else{
				message.ini_sendPort(next);
				message.ini_selfPort(myPort);
				new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,message);
				Log.v(TAG,"Send resquest delete"+message.key+"to:"+next+"After detect failure: "+sendPort);
			}
		}

		else if(name.equals("re_QueryALL")){
			String nextPort = circle.getSuccess(sendPort);
			message.ini_sendPort(nextPort);
			new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,message);
			Log.v(TAG,"Send query all to: "+ nextPort+"After detect failure: "+sendPort);
		}

		else if(name.equals("re_deleteAll")){
			String nextPort = circle.getSuccess(sendPort);
			message.ini_sendPort(nextPort);
			new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,message);
			Log.v(TAG,"Send delete all to: "+ nextPort+"After detect failure: "+sendPort);

		}
		e.printStackTrace();
	}

	public String checkbelongto(String key){
           if(getLocation(key).compareTo(getPortLocation("11124")) < 0 || getLocation(key).compareTo(getPortLocation("11120")) > 0){
           	  return "11124";
		   }
		   else if(getLocation(key).compareTo(getPortLocation("11112")) < 0 && getLocation(key).compareTo(getPortLocation("11124")) > 0){
           	  return "11112";
		   }
		   else if(getLocation(key).compareTo(getPortLocation("11108")) < 0 && getLocation(key).compareTo(getPortLocation("11112")) > 0){
		   	  return "11108";
		   }
		   else if(getLocation(key).compareTo(getPortLocation("11116")) < 0 && getLocation(key).compareTo(getPortLocation("11108")) > 0){
			   return "11116";
		   }
		   else{
		   	   return "11120";
		   }

	}
}
