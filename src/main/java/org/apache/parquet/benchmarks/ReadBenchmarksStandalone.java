/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.benchmarks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.Page;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import static org.apache.parquet.benchmarks.BenchmarkConstants.*;
import static org.apache.parquet.benchmarks.BenchmarkFiles.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadBenchmarksStandalone {
	
	public static void main(String[] args) {
		String path = "part-m-00005.snappy.parquet";
		System.out.println(System.currentTimeMillis());
		read(path);
		System.out.println(System.currentTimeMillis());

//		Map<String, Integer> counts = new HashMap<String, Integer>();
////		read(args[0], counts);
//		long st1 = System.currentTimeMillis();
//		parquetReadByGroup(args[0]);
//		long end1 = System.currentTimeMillis();
////		read("part-m-00005.snappy.parquet", counts);
//		parquetRead(args[0]);
//		long end2 = System.currentTimeMillis();
//		System.out.println("Parquet Read by Group totle time: " +(end1 - st1) + "ms");
//		System.out.println("Parquet Read by Page  totle time: " +(end2 - end1) + "ms");
	}
	
	
	public static void parquetRead(String inputPath){
		for(int i = 0; i <= 5; i++){
			String path = inputPath + i + ".snappy.parquet";
			Path parquetFile = new Path(path);
			try {
				readColumn(parquetFile, configuration, 1);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void parquetReadByGroup(String inputPath){
		for(int i = 0; i <= 5; i++){
			String path = inputPath + i + ".snappy.parquet";
			read(path);
		}
	}
	
	public static void readColumn(Path testFile, Configuration configuration, int colsToLoad) throws IOException {
		long t0 = System.currentTimeMillis();
		StringBuilder schemaString = new StringBuilder("message sample { required binary city; }");
//		for (int i = 1; i < colsToLoad; i++) {
//			schemaString.append("required binary a" + i + ";");
//		}
//		schemaString.append(" }");
		MessageType schema = MessageTypeParser.parseMessageType(schemaString.toString());
		List<ColumnDescriptor> columns = schema.getColumns();

		ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, testFile);
		ParquetFileReader parquetFileReader = new ParquetFileReader(configuration, testFile, readFooter.getBlocks(),
				schema.getColumns());
		PageReadStore pages;
		do {
			pages = parquetFileReader.readNextRowGroup();
			if(pages == null) {
				break;
			}
			for (ColumnDescriptor col : columns) {
				PageReader reader = pages.getPageReader(col);
				Page page;
				do {
					page = reader.readPage();
					 if (page != null) {
		                    String data = new String(((DataPageV1) page).getBytes().toByteArray(), BytesUtils.UTF8);
//		                    System.out.print("data:" + data);
		                }
//					System.out.println(page);
				} while (page != null);
			}
		} while (pages != null);
	}
	
public static void read(String path) {
		
		Path parquetFile = new Path(path);
		ParquetReader<Group> reader = null;

	StringBuilder schemaString  = new StringBuilder("message sample { required int32 city; }");
	MessageType schema  = MessageTypeParser.parseMessageType(schemaString.toString());
		GroupReadSupport groupReadSupport = new GroupReadSupport();
		groupReadSupport.init(configuration, new HashMap<String, String>(), schema);
		try {
			reader = ParquetReader.builder(groupReadSupport, parquetFile).withConf(configuration).build();
			} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
			Group group = null;
			try {
				group = reader.read();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			int counter = 0;
			while(group != null){
				counter++;
//				String cityCode = "";
				int  cityCode = group.getInteger("city",0);
				cityCode += 1;
//				if(counts.containsKey(cityCode)){
//					counts.put(cityCode, counts.get(cityCode) + 1);
//				}else {
//					counts.put(cityCode, 1);
//				}
				try {
					group = reader.read();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			System.out.println(counter);
		
	}
	
	public static void read(String path, Map<String, Integer> counts) {
		
		Path parquetFile = new Path(path);
		ParquetReader<Group> reader = null;
		try {
			reader = ParquetReader.builder(new GroupReadSupport(), parquetFile).withConf(configuration).build();
			} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
			Group group = null;
			try {
				group = reader.read();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			int counter = 0;
			while(group != null){
				counter++;
				String cityCode = "";
				cityCode = group.getString("ip",0);
				
				if(counts.containsKey(cityCode)){
					counts.put(cityCode, counts.get(cityCode) + 1);
				}else {
					counts.put(cityCode, 1);
				}
				try {
					group = reader.read();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			System.out.println(counter);
		
	}
	
  private void read(Path parquetFile, int nRows, Blackhole blackhole) throws IOException
  {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), parquetFile).withConf(configuration).build();
    for (int i = 0; i < nRows; i++) {
      Group group = reader.read();
      blackhole.consume(group.getBinary("binary_field", 0));
      blackhole.consume(group.getInteger("int32_field", 0));
      blackhole.consume(group.getLong("int64_field", 0));
      blackhole.consume(group.getBoolean("boolean_field", 0));
      blackhole.consume(group.getFloat("float_field", 0));
      blackhole.consume(group.getDouble("double_field", 0));
      blackhole.consume(group.getBinary("flba_field", 0));
      blackhole.consume(group.getInt96("int96_field", 0));
    }
    reader.close();
  }

  @Benchmark
  public void read1MRowsDefaultBlockAndPageSizeUncompressed(Blackhole blackhole)
          throws IOException
  {
    read(file_1M, ONE_MILLION, blackhole);
  }

  @Benchmark
  public void read1MRowsBS256MPS4MUncompressed(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_BS256M_PS4M, ONE_MILLION, blackhole);
  }

  @Benchmark
  public void read1MRowsBS256MPS8MUncompressed(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_BS256M_PS8M, ONE_MILLION, blackhole);
  }

  @Benchmark
  public void read1MRowsBS512MPS4MUncompressed(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_BS512M_PS4M, ONE_MILLION, blackhole);
  }

  @Benchmark
  public void read1MRowsBS512MPS8MUncompressed(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_BS512M_PS8M, ONE_MILLION, blackhole);
  }

  //TODO how to handle lzo jar?
//  @Benchmark
//  public void read1MRowsDefaultBlockAndPageSizeLZO(Blackhole blackhole)
//          throws IOException
//  {
//    read(parquetFile_1M_LZO, ONE_MILLION, blackhole);
//  }

  @Benchmark
  public void read1MRowsDefaultBlockAndPageSizeSNAPPY(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_SNAPPY, ONE_MILLION, blackhole);
  }

  @Benchmark
  public void read1MRowsDefaultBlockAndPageSizeGZIP(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_GZIP, ONE_MILLION, blackhole);
  }
}
