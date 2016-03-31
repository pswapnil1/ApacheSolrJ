package Haddock.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Hit;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;

public class listdirectories {
	public static final String FILES_TO_INDEX_DIRECTORY = "hdfs://192.168.1.1:8020/data/20140404"; //Your Hadoop Server
	public static final String INDEX_DIRECTORY = "hdfs://192.168.1.1:8020/data/data_index/";
	public static final String FIELD_PATH = "path";
	public static final String FIELD_CONTENTS = "contents";
	public static void main(String[] args) throws IOException, ParseException {
	
		String srcPath = FILES_TO_INDEX_DIRECTORY;
		Path p = new Path(srcPath);
		Configuration config = new Configuration();
		config.set("fs.default.name","hdfs://192.168.1.1:8020/");
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriter indexWriter = new IndexWriter(INDEX_DIRECTORY, analyzer, true);
		list(p,config,indexWriter);
	 	indexWriter.close();
		
	 	searchIndex("AAPL"); // Search query.
		searchIndex("Apple");
		searchIndex("531.8000");  
		searchIndex("document");  
		searchIndex("Parquet");
	 }

	 public static void list(Path path,Configuration config,IndexWriter indexWriter) throws FileNotFoundException, IllegalArgumentException, IOException
	 {
		 FileSystem fs = null;
    	 try {
			
   			fs = FileSystem.get(config);
   			FileStatus[] status = fs.listStatus(path);  // you need to pass in your hdfs path
   			for (FileStatus fileStatus : status) {

				if(fileStatus.isDirectory()){
	         		 list(fileStatus.getPath(),config,indexWriter);
				}
				else{
					System.out.println("file path "+fileStatus.getPath().toString());
					Document document = new Document();
					document.add(new Field(FIELD_PATH, fileStatus.getPath().toString(), Field.Store.YES, Field.Index.UN_TOKENIZED));
				    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
					document.add(new Field(FIELD_CONTENTS, br));
					indexWriter.addDocument(document);
					indexWriter.commit();
					indexWriter.optimize();
					System.out.println("Folder [ "+fileStatus.getPath().getParent() + " ] --->" + fileStatus.getPath());
				}
		}
   	 } catch (IOException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		}
	}
	 public static void searchIndex(String searchString) throws IOException, ParseException {
 			System.out.println("Searching for '" + searchString + "'");
			Directory directory = FSDirectory.getDirectory(INDEX_DIRECTORY);
			IndexReader indexReader = IndexReader.open(directory);
			IndexSearcher indexSearcher = new IndexSearcher(indexReader);
			Analyzer analyzer = new StandardAnalyzer();
			QueryParser queryParser = new QueryParser(FIELD_CONTENTS, analyzer);
			Query query = queryParser.parse(searchString);
			Hits hits = indexSearcher.search(query);
			System.out.println("Number of hits: " + hits.length());
				Iterator<Hit> it = hits.iterator();
				while (it.hasNext()) {
					Hit hit = it.next();
					Document document = hit.getDocument();
					String path = document.get(FIELD_PATH);
					System.out.println("Hit: " + path);
					//System.out.println("roate");
				}
		}
}



        
    
