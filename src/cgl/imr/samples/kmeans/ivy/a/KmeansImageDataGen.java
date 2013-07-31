/*
 * Software License, Version 1.0
 *
 *  Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 *
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1) All redistributions of source code must retain the above copyright notice,
 *  the list of authors in the original source code, this list of conditions and
 *  the disclaimer listed in this license;
 * 2) All redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the disclaimer listed in this license in
 *  the documentation and/or other materials provided with the distribution;
 * 3) Any documentation included with all redistributions must include the
 *  following acknowledgement:
 *
 * "This product includes software developed by the Community Grids Lab. For
 *  further information contact the Community Grids Lab at
 *  http://communitygrids.iu.edu/."
 *
 *  Alternatively, this acknowledgement may appear in the software itself, and
 *  wherever such third-party acknowledgments normally appear.
 *
 * 4) The name Indiana University or Community Grids Lab or Twister,
 *  shall not be used to endorse or promote products derived from this software
 *  without prior written permission from Indiana University.  For written
 *  permission, please contact the Advanced Research and Technology Institute
 *  ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 * 5) Products derived from this software may not be called Twister,
 *  nor may Indiana University or Community Grids Lab or Twister appear
 *  in their name, without prior written permission of ARTI.
 *
 *
 *  Indiana University provides no reassurances that the source code provided
 *  does not infringe the patent or any other intellectual property rights of
 *  any other entity.  Indiana University disclaims any liability to any
 *  recipient for claims brought by any other entity based on infringement of
 *  intellectual property rights or otherwise.
 *
 * LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO
 * WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 * NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF
 * INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS.
 * INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS",
 * "VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.
 * LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR
 * ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION
 * GENERATED USING SOFTWARE.
 */

package src.cgl.imr.samples.kmeans.ivy.a;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.KeyValuePair;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.types.IntKey;
import cgl.imr.types.MemCacheAddress;
import cgl.imr.types.StringValue;

/**
 * Generate data for K-Means image clustering using MapReduce. It uses a "map-only"
 * operation to generate data concurrently.
 * 
 * @author Fei Teng (tf.richard@gmail.com)
 * 
 */
public class KmeansImageDataGen {

	//public static String DATA_FILE_SUFFIX = ".data";
	//public static String PROP_NUM_PICTURE_PER_MAP = "num_pictures_per_map";
	//public static String NUM_CENTROIDS = "num_centroids";
	public static String NUMPICS = "num_pics_per_map";
	public static String NUMROWS = "num_rows_per_map";
	public static String NUMCOLS = "num_cols_per_map";
	public static String NUMDIMS = "num_dims_per_map";
	
	public static String Dims = "dims";
	public static String Rows = "rows";
	public static String Cols = "cols";
	public static String Pics = "pictures";
	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	public static void generateInitClusterCenters(String initCenterFile, int numPics, int numRows, int numCols,
			int dims) {
		
		System.out.println("Pics: " + numPics + " numRow: " + 
				numRows + " numCols: " + numCols +  " dimensions: " + dims);
		Random rand = new Random(System.nanoTime());
		try {
			DataOutputStream writer = new DataOutputStream(
					new BufferedOutputStream(new FileOutputStream(
							initCenterFile)));

			// String line = numCentroids + "\n";
			// writer.write(line);
			
			for (int i = 0; i < numPics; i++) {
				for (int j = 0; j < numRows; j++) {
					for (int k = 0; k < numCols; k++) {
						
						String line = i + " " + j + " " + k + "\t" + dims;
						for (int dim = 0; dim < dims; dim ++) {
							line += " " + rand.nextInt(255);
						}
						writer.writeChars(line);
					}
					writer.flush();
				}
			}
			
			
			//writer.writeInt(numCentroids);

			// line = vectLength + "\n";
			// writer.write(line);
			//writer.writeInt(dims);

//			for (int i = 0; i < numCentroids; i++) {
//				/*
//				 * line = ""; for (int j = 0; j<vectLength; j++){ line +=
//				 * rand.nextInt(KmeansDataGenMapTask.MAX_VALUE)+" "; } line +=
//				 * "\n"; writer.write(line);
//				 */
//				for (int j = 0; j < dims; j++) {
//					writer.writeDouble(rand.nextDouble()*KmeansDataGenMapTask.MAX_VALUE);
//				}
//			}
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Produces a list of key,value pairs for map tasks.
	 * 
	 * @param numMaps
	 *            - Number of map tasks.
	 * @return - List of key,value pairs.
	 */
//	private static List<KeyValuePair> getKeyValuesForMap(int numMaps,
//			String dataFilePrefix, String dataDir) {
//		List<KeyValuePair> keyValues = new ArrayList<KeyValuePair>();
//		IntKey key = null;
//		StringValue value = null;
//		for (int i = 0; i < numMaps; i++) {
//			key = new IntKey(i);
//			value = new StringValue(dataDir + "/" + dataFilePrefix + i
//					+ DATA_FILE_SUFFIX);
//			keyValues.add(new KeyValuePair(key, value));
//		}
//		return keyValues;
//	}

	public void driveMapReduce(String[] args) throws Exception {
		// int numCentroids = Integer.parseInt(args[1]);
		// int vecLength = Integer.parseInt(args[2]);
		//String dataDir = args[3];
		//String dataFilePrefix = args[4];
//		int numMapTasks = Integer.parseInt(args[5]);
//		long numDataPoints = Long.parseLong(args[6]);
//		long numDataPointsPerMap = numDataPoints / numMapTasks;
		
		int numPics = Integer.parseInt(args[1]);
		int numRows = Integer.parseInt(args[2]);
		int numCols = Integer.parseInt(args[3]);
		int dims = Integer.parseInt(args[4]);
		int numMappers = Integer.parseInt(args[5]);
		String partitionFile = args[6];

//		if(numDataPointsPerMap > Integer.MAX_VALUE) {
//			System.out.println("numDataPointsPerMap Exceed Integer Max Value");
//		}
		int numReducers = 0; // We don't need any reducers.

		// JobConfigurations
		JobConf jobConf = new JobConf("kmeans-image-data-gen"
				+ uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(KmeansImageDataGenMapTask.class);
		jobConf.setNumMapTasks(numMappers);
		jobConf.setNumReduceTasks(numReducers);
		jobConf.addProperty(NUMPICS,
				String.valueOf(numPics));
		jobConf.addProperty(NUMROWS, String.valueOf(numRows));
		jobConf.addProperty(NUMCOLS, String.valueOf(numCols));
		jobConf.addProperty(NUMDIMS, String.valueOf(dims));
		jobConf.setFaultTolerance();
		TwisterDriver driver = new TwisterDriver(jobConf);
		driver.configureMaps(partitionFile);
		
		List<Value> bcastValue = new ArrayList<Value>();
		StringValue dummy = new StringValue("");
		bcastValue.add(dummy);
		
		MemCacheAddress memCacheKay = driver.addToMemCache(bcastValue);
		TwisterMonitor monitor = driver.runMapReduceBCast(memCacheKay);
		monitor.monitorTillCompletion();
		driver.cleanMemCache();
		driver.close();
	}

	public static void main(String[] args) throws Exception {
		System.out.println("kmeans args.len:" + args.length);
		if (args.length != 7) {
			String errorReport = "KmeansDataGen: The Correct arguments are \n"
					+ "java cgl.mr.kmeans.KmeansDataGen  [init clusters file][num centroids][vector length][sub dir][data file prefix][num splits=num maps][num data points]";
			System.out.println(errorReport);
			System.exit(0);
		}

		String initClustersFile = args[0];
		int numPics = Integer.parseInt(args[1]);
		int numRows = Integer.parseInt(args[2]);
		int numCols = Integer.parseInt(args[3]);
		int numDims = Integer.parseInt(args[4]);
		//int numMappers = Integer.parseInt(args[5]);
		//String partitionFile = args[6];

		// Generate initial cluster centers sequentially.
		generateInitClusterCenters(initClustersFile, numPics, numRows, numCols, numDims);

		try {
			KmeansImageDataGen client = new KmeansImageDataGen();
			client.driveMapReduce(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}
}
