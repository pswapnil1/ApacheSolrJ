package com.gridedge.lucene_MR.Search_LuceneMR;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Hello world!
 *
 */
public class App 
{
	public static class indexmapper extends Mapper<Object, Text, Text, IntWritable> {
		FileSystem fs = null;
		String HDFS_DATA_DIRECTORY = "hdfs://192.168.1.15:8020/data/market/stocks/intraDayYahoo/AAPL/20140404/";
		final String FIELD_PATH = "path";
		private Text word = new Text();
				public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
							System.out.println("I am in Map Method");
							StringTokenizer itr = new StringTokenizer(value.toString());
							System.out.println("I am in Map method");
							String indexDirectory = "/home/huser/index3/";
							Directory dir1 = new SimpleFSDirectory(new File(indexDirectory));
							Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
							IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_47, analyzer);
							IndexWriter indexWriter = new IndexWriter(dir1, indexWriterConfig);

							Configuration config = new Configuration();
							config.set("fs.default.name","hdfs://192.168.1.15:8020/");
							FileSystem dfs = FileSystem.get(config);
							FileStatus[] status = dfs.listStatus(new Path("hdfs://192.168.1.15:8020/data/market/stocks/intraDayYahoo/AAPL/20140404/"));
							
							for(int i=0; i<status.length;i++) {
								
							
									while (itr.hasMoreTokens()) {
									word.set(itr.nextToken());
									Document document = new Document();
									document.add(new Field(FIELD_PATH, status[i].getPath().toString(), Field.Store.YES, Field.Index.ANALYZED));
									indexWriter.addDocument(document);
									indexWriter.commit();
									//context.write(word, one);
									}
							}
							indexWriter.close();
			}
		
		}
				
	public static class indexreducer  extends Reducer<Text, IntWritable, Text, IntWritable> {

			final String FIELD_CONTENTS = "contents";
			final private Text result = new Text();

			public void reduce(Text key, Iterable<IntWritable> values,Context context)  throws IOException, InterruptedException {
			// TODO Auto-generated method stub
				System.out.println("I am in Reduce Method");
			String translations = "";
			Configuration cfg = new Configuration();
			cfg.set("fs.default.name", "hdfs://192.168.1.15:8020/");
			System.out.println("I am in Reducere meyhods");
			FileSystem fs = null;
			fs = FileSystem.get(cfg); // FOR HDFS file system

			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
			String indexDirectory = "/home/huser/index3/";
			//boolean recreateIndexIfExists = true;
			Directory dir = new SimpleFSDirectory(new File(indexDirectory));
			IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_47, analyzer);
			IndexWriter indexWriter = new IndexWriter(dir, indexWriterConfig);

			//int sum = 0;
			for (IntWritable val : values) {
				//sum += val.get();
				Document document = new Document();
				System.out.println(values.toString());
				document.add(new Field(FIELD_CONTENTS, values.toString(), Field.Store.YES, Field.Index.ANALYZED));
		
				indexWriter.addDocument(document);
				indexWriter.commit();
			}
			//result.set(sum);
			indexWriter.close();
			 
		}

	}


		public static void main(String[] args) throws Exception {
			
			String HDFS_DATA_DIRECTORY = "hdfs://192.168.1.15:8020/data/market/stocks/intraDayYahoo/AAPL/20140404/";
			String LOCAL_INDEX_DIRECTORY = "/home/huser/index3/";

			Path inp = new Path(HDFS_DATA_DIRECTORY);
			Path out = new Path(LOCAL_INDEX_DIRECTORY);
			
			System.out.println("I am in Main Method");
			
			Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Lucene MapReduce");
			
		    job.setJarByClass(App.class);
		    job.setMapperClass(indexmapper.class);
		    job.setCombinerClass(indexreducer.class);
		    job.setReducerClass(indexreducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, inp);
		    FileOutputFormat.setOutputPath(job, out);
		    App.searchIndex("Apple");
		    //App.searchIndex("AAPL");
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		   
		    
		 	}
		
		public static void searchIndex(String searchString) throws IOException, ParseException 
		{
					System.out.println("Searching for '" + searchString + "'");
					
					String FIELD_CONTENTS = "contents";
					String indexDirectory = "/home/huser/index2/";
					System.out.println("Path is ::::"+indexDirectory);
					Directory directory = FSDirectory.open(new File(indexDirectory));
					IndexReader indexReader = IndexReader.open(directory);
					System.out.println("Index Dir :"+indexReader);
					IndexSearcher indexSearcher = new IndexSearcher(indexReader);
					Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
					QueryParser queryParser = new QueryParser(Version.LUCENE_47,FIELD_CONTENTS, analyzer);
					Query query = queryParser.parse(searchString);
					long start = System.currentTimeMillis();
					TopDocs hits = indexSearcher.search(query, 100);
					long end = System.currentTimeMillis();
					System.out.println("Number of hits: " + hits.totalHits);
					System.err.println("Found " + hits.totalHits + " document(s) in " +
					(end-start) + " milliseconds");
					List<String> pathList = new ArrayList<String>();
					ScoreDoc[] scoreDocs = hits.scoreDocs;
					for (ScoreDoc scoreDoc : hits.scoreDocs){
							Document document = indexSearcher.doc(scoreDoc.doc);
							System.out.println("Path is :"+document.get("path"));
							pathList.add(document.get("path"));

					}
			
			//return pathList;

		}
		
}
