package com.ligadata.dataGenerationTool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;

import com.ligadata.dataGenerationTool.bean.ConfigObj;
import com.ligadata.dataGenerationTool.bean.FileNameConfig;

import org.apache.log4j.Logger;

import java.util.zip.GZIPOutputStream;

public class FilesUtility {

	final Logger logger = Logger.getLogger(FilesUtility.class);

	public void writeFile(String record, String path, ConfigObj configObj,
			FileNameConfig fileNameConfig) {

		TimeUtility time = new TimeUtility();
		String fileSplitPer = configObj.getFileSplitPer();
		String fileNameFormat = time.CheckTimeUnit(fileSplitPer
				.substring(fileSplitPer.length() - 1));

		double currentTime = System.currentTimeMillis();

		try {
			String fileName;
			if (fileNameConfig.getOldFileTime() == 0) {
				fileNameConfig.setOldFileTime(currentTime);
			}
			// String reportDate = df.format(today);

			if (time.CreateNewFile(configObj, fileNameConfig, currentTime)) {
				fileName = new SimpleDateFormat(fileNameFormat)
						.format(currentTime);
				logger.info("writing on " + fileName);
			} else {
				fileName = new SimpleDateFormat(fileNameFormat)
						.format(fileNameConfig.getOldFileTime());
			}

			File file = new File(path + fileName);

			// if compression is needed then write to compressed file, else
			// write to normal file.
			if (configObj.getCompressFormat().equalsIgnoreCase("gzip")) {
				File compressedFile = new File(path + fileName + ".gzip");
				if (!(compressedFile.exists())) {
					compressedFile.createNewFile();
				}

				BufferedWriter writer = null;

				GZIPOutputStream gzip = new GZIPOutputStream(new FileOutputStream(compressedFile, true));
				writer = new BufferedWriter(new OutputStreamWriter(gzip,"UTF-8"));
				writer.append(record + "\n");
				if (writer != null) {
					writer.close();
				}

			} else {
				if (!(file.exists())) {
					file.createNewFile();
				}
				FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
				BufferedWriter bw = new BufferedWriter(fw);
				bw.write(record);
				bw.newLine();
				bw.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error(e);
		}

	}

	public static void CompressFileGZIP(String sourceFile,
			String destinationFile, ConfigObj confObj) {

		byte[] buffer = new byte[1024];

		try {
			GZIPOutputStream gzos = new GZIPOutputStream(new FileOutputStream(
					destinationFile));

			FileInputStream in = new FileInputStream(sourceFile);

			int len;
			while ((len = in.read(buffer)) > 0) {
				gzos.write(buffer, 0, len);
			}

			in.close();
			gzos.finish();
			gzos.close();

			System.out.println("Done");
		} catch (IOException ex) {
			ex.printStackTrace();
		}

	}

}
