package com.ericsson.eniq.etl.mrr;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xml.sax.helpers.DefaultHandler;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.parser.Main;
import com.distocraft.dc5000.etl.parser.MeasurementFile;
import com.distocraft.dc5000.etl.parser.Parser;
import com.distocraft.dc5000.etl.parser.SourceFile;

/**
* <br>
* <table border="1" width="100%" cellpadding="3" cellspacing="0">
* <tr bgcolor="#CCCCFF" class="TableHeasingColor">
* <td colspan="4"><font size="+2"><b>Parameter Summary</b></font></td>
* </tr>
* <tr>
* <td><b>Key</b></td>
* <td><b>Description</b></td>
* <td><b>Default</b></td>
* </tr>
* <tr>
* <td>row_delimiter</td>
* <td>Row Delimiter</td>
* <td>\n</td>
* </tr>
* <tr>
* <td>column_delimiter</td>
* <td>Column Delimiter</td>
* <td>:</td>
* </tr>
* <tr>
* <td>multi_value_delimiter</td>
* <td>The delimiter to be used to separate multi data values in the file.</td>
* <td>;</td>
* </tr>
* <tr>
* <td>header_delimiter</td>
* <td>The delimiter used in the file that separates the Block Header String and the Block Header Number</td>
* <td>-</td>
* </tr>
* <tr>
* <td>header_pattern_string</td>
* <td>The Pattern used by the parser to find the Block Headers in the file.</td>
* <td>^.*?\header_delimiter\s*[0-9]+$</td>
* </tr>
* <tr>
* <td>datetime_mode</td>
* <td>Defines the source for the DATETIME_ID Column.
* <br>1 – DATETIME_ID is to be extracted from the file name. In this case, datetime_column should provide the Pattern to be used by the parser for extraction.
* <br>2 - DATETIME_ID is to be read from a single key. In this case, datetime_column should specify the Key from which data needs to be extracted.</td>
* <td>1</td>
* </tr>
* <tr>
* <td>datetime_column</td>
* <td>If datetime_mode set to 1, then it specifies the pattern to be used to extract DATETIME_ID from filename.
* <br>If datetime_mode set to 2, then  it specifies the Key from which data needs to be extracted</td>
* <td>NULL</td>
* </tr>
* <tr>
* <td>start_datetime_mode</td>
* <td>Defines the source for the START_DATETIME Column.
* <br>1 – START_DATETIME is to be read from multiple keys.  If set, start_date_col and start_time_col properties should specify the keys from which START_DATETIME needs to be extracted.
* <br>2 - START_DATETIME is to be read from a single key. In this case, start_datetime_column should specify the Key from which data needs to be extracted.</td>
* <td>1</td>
* </tr>
* <tr>
* <td>start_datetime_column</td>
* <td>If datetime_mode set to 2, then  it specifies the Key from which data needs to be extracted</td>
* <td>NULL</td>
* </tr>
* <tr>
* <td>start_date_col</td>
* <td>Specified the Key to extract the Date for START_DATETIME if start_datetime_mode is set to 1</td>
* <td>NULL</td>
* </tr>
* <tr>
* <td>start_time_col</td>
* <td>Specified the Key to extract the Time for START_DATETIME if start_datetime_mode is set to 1</td>
* <td>NULL</td>
* </tr>
* <tr>
* <td>calculate_end_date_time</td>
* <td>The parser calculates the End Date Time ID if set to “True”
* <br>If set, total_recording_time_col should specify the Key.</td>
* <td>False</td>
* </tr>
* <tr>
* <td>total_recording_time_col</td>
* <td>Specifies the Key within the File for Total Recording Time.</td>
* <td>NULL</td>
* </tr>
* <tr>
* <td>source_date_format</td>
* <td>Specifies the DateTIme format in the file.</td>
* <td>yy/MM/dd HH:mm:ss</td>
* </tr>
* <tr>
* <td>target_date_format</td>
* <td>Specifies the target DateTIme format.</td>
* <td>yyyy-MM-dd HH:mm:ss</td>
* </tr>
* <tr>
* <td>last_block_id_to_skip</td>
* <td>Specifies the Block Header Number to be skipped if the Block is present at the end of the file.
*  <br>(e.g.: Specify 30 for ADM Block in MRR File)</td>
* <td>NULL</td>
* </tr>
* <tr>
* <td>mandatory_keys</td>
* <td>Comma separated list of all the mandatory keys that are to be included with each MO</td>
* <td>NULL</td>
* </tr>
* <tr>
* <td>key_pairs_in_single_line</td>
* <td>: separated pairs of keys that occur within a same line. Each pair is separated by a ;</td>
* <td>NULL</td>
* </tr>
**/

public class MRRParser extends DefaultHandler implements Parser {
	
	private static final String JVM_TIMEZONE = new SimpleDateFormat("Z").format(new Date());
	public static final int DATETIME_FROM_FILENAME = 1;
	public static final int DATETIME_FROM_SINGLE_COLUMN = 2;
	public static final int START_DATETIME_FROM_MULTI_COLUMN = 1;
	public static final int START_DATETIME_FROM_SINGLE_COLUMN = 2;
	
	private String block = "";
	private int bufferSize = 10000;
	private int rowDelimLength = 1;
	private BufferedReader br;
	private SourceFile sf;
	private String filename;
	private Logger log;
	private String rowDelim;
	private String colDelim;
	private String multiValDelim;
	private String headerDelim;
	private String headerPatternStr;
	private Pattern headerPattern;
	private String headerNumber = "";
	private int dateTimeMode;
	private String dateTimeColumn;
	private String dateTimeIdData = "";
	private int startDateTimeMode;
	private String startDateTimeColumn;
	private String startDateCol;
	private String startTimeCol;
	private String startDateTimeIdData = "";
	private String calculateEndDateTime;
	private String totalRecordingTimeCol;
	private int totalRecordingTime;
	private String sourceDateFormat;
	private String targetDateFormat;
	private String lastBlockIdToSkip;
	private String mandatoryKeys;
	private String keyPairsInSingleLine;
	private Map<String,String> mandatoryKeyValuePair = null;
	Map<String,String> keyPairsMap = null;
	private MeasurementFile mFile = null;
	
	//****** Parameters for throughput measurement *******
	private long parseStartTime;
	private long totalParseTime;
	private long fileSize;
	private int fileCount;
	
	//****************** Worker stuff *********************
	private String techPack;
	private String setType;
	private String setName;
	private int status = 0;
	private Main mainParserObject = null;
	private String workerName = "";

	@Override
	public void init(final Main main, final String techPack, final String setType, final String setName, final String workerName) {
		this.mainParserObject = main;
		this.techPack = techPack;
		this.setType = setType;
		this.setName = setName;
		this.status = 1;
		this.workerName = workerName;

		String logWorkerName = "";
		if (workerName.length() > 0) {
			logWorkerName = "." + workerName;
		}

		log = Logger.getLogger("etl." + techPack + "." + setType + "." + setName + ".parser.MRRParser" + logWorkerName);
	}

	@Override
	public void run() {

		try {

			this.status = 2;
			SourceFile sf = null;			
			parseStartTime = System.currentTimeMillis();
			while ((sf = mainParserObject.nextSourceFile()) != null) {

				try {
					fileCount++;
					fileSize += sf.fileSize();
					mainParserObject.preParse(sf);
					parse(sf, techPack, setType, setName);
					mainParserObject.postParse(sf);
				} catch (Exception e) {
					mainParserObject.errorParse(e, sf);
				} finally {
					mainParserObject.finallyParse(sf);
				}
			}
			
			totalParseTime = System.currentTimeMillis() - parseStartTime;
			if (totalParseTime != 0) {
				log.info("Parsing Performance :: " + fileCount + " files parsed in " + totalParseTime
						+ " milliseconds, filesize is " + fileSize + " bytes and throughput : " + (fileSize / totalParseTime)
						+ " bytes/ms.");
			}
		} catch (Exception e) {
			// Exception catched at top level.
			log.log(Level.WARNING, "Worker parser failed with exception", e);
		} finally {
			this.status = 3;
		}
	}

	@Override
	public int status() {
		return status;
	}

	
	@Override
	public void parse(final SourceFile sf, final String techPack, final String setType, final String setName)
			throws Exception {
		
		String line;
		this.sf = sf;
		this.filename = sf.getName();
		this.block = "";
		this.startDateTimeIdData = "";
		
		try {
			rowDelim = sf.getProperty("row_delimiter", "\n");
			if (rowDelim.length() == 0) {
				rowDelim = "\n";
			}
			log.finest("row_delim: " + rowDelim);
			
			colDelim = sf.getProperty("column_delimiter", ":");
			if (colDelim.length() == 0) {
				colDelim = ":";
			}
			log.finest("column_delim: " + colDelim);
			
			multiValDelim = sf.getProperty("multi_value_delimiter", ";");
			if (multiValDelim.length() == 0) {
				multiValDelim = ";";
			}
			log.finest("multi_value_delim: " + multiValDelim);
			
			headerDelim = sf.getProperty("header_delimiter", "-");
			if (headerDelim.length() == 0) {
				headerDelim = "-";
			}
			log.finest("header_delim: " + headerDelim);
			
			headerPatternStr = sf.getProperty("header_pattern_string", "^.*?\\" + headerDelim + "\\s*[0-9]+$");
			if (headerPatternStr.length() == 0) {
				headerPatternStr = "^.*?\\" + headerDelim + "\\s*[0-9]+$";
			}
			log.finest("header_pattern_string: " + headerPatternStr);
			headerPattern = Pattern.compile(headerPatternStr, Pattern.MULTILINE);
			
			dateTimeMode = Integer.parseInt(sf.getProperty("datetime_mode", DATETIME_FROM_FILENAME + ""));
			log.finest("datetime_mode: " + dateTimeMode);
			
			dateTimeColumn = sf.getProperty("datetime_column", "");
			if (dateTimeColumn.length() == 0) {
				dateTimeColumn = "";
			}
			log.finest("datetime_column: " + dateTimeColumn);
			
			try {
				if (dateTimeMode == DATETIME_FROM_FILENAME) {
					final String patt = dateTimeColumn;
					final Matcher m = Pattern.compile(patt).matcher(filename);
					if (m.find()) {
						dateTimeColumn = m.group(1);
					}
				}
			} catch (Exception e) {
				log.log(Level.WARNING, "Error while matching pattern " + dateTimeColumn + " on filename " + filename + " for DATETIME_ID", e);
			}
			
			startDateTimeMode = Integer.parseInt(sf.getProperty("start_datetime_mode", START_DATETIME_FROM_MULTI_COLUMN + ""));
			log.finest("start_datetime_mode: " + startDateTimeMode);
			
			startDateTimeColumn = sf.getProperty("start_datetime_column", "");
			if (startDateTimeColumn.length() == 0) {
				startDateTimeColumn = "";
			}
			log.finest("start_datetime_column: " + startDateTimeColumn);
			
			startDateCol = sf.getProperty("start_date_col", "");
			log.finest("start_date_col: " + startDateCol);
			
			startTimeCol = sf.getProperty("start_time_col", "");
			log.finest("start_time_col: " + startTimeCol);
			
			calculateEndDateTime = sf.getProperty("calculate_end_date_time", "false");
			log.finest("calculate_end_date_time: " + calculateEndDateTime);
			
			totalRecordingTimeCol = sf.getProperty("total_recording_time_col", "");
			log.finest("total_recording_time_col: " + totalRecordingTimeCol);
			
			sourceDateFormat = sf.getProperty("source_date_format", "yy/MM/dd HH:mm:ss");
			if (sourceDateFormat.length() == 0) {
				sourceDateFormat = "yy/MM/dd HH:mm:ss";
			}
			log.finest("source_date_format: " + sourceDateFormat);
			
			targetDateFormat = sf.getProperty("target_date_format", "yyyy-MM-dd HH:mm:ss");
			if (targetDateFormat.length() == 0) {
				targetDateFormat = "yyyy-MM-dd HH:mm:ss";
			}
			log.finest("target_date_format: " + targetDateFormat);
			
			lastBlockIdToSkip = sf.getProperty("last_block_id_to_skip", "");
			log.finest("last_block_id_to_skip: " + lastBlockIdToSkip);
			
			mandatoryKeys = sf.getProperty("mandatory_keys", "");
			log.finest("mandatory_keys: " + mandatoryKeys);
			if (mandatoryKeys != null && !mandatoryKeys.isEmpty()) {
				mandatoryKeyValuePair = new HashMap<String,String>();
				
				for (String key : mandatoryKeys.split(",")) {
					mandatoryKeyValuePair.put(key.trim(),null);
				}
			}
			
			keyPairsInSingleLine = sf.getProperty("key_pairs_in_single_line", "");
			log.finest("key_pairs_in_single_line: " + keyPairsInSingleLine);
			// e.g.: key_pairs_in_single_line = "TNCCPERM bitmap:NCCs permitted;..."
			if (keyPairsInSingleLine != null && !keyPairsInSingleLine.isEmpty()) {
				keyPairsMap = new HashMap<String,String>();
				for (String keys : keyPairsInSingleLine.split(";")) {
					if (!keys.isEmpty()) {
						String key[] = keys.split(":");
						if (key.length > 1) {
							keyPairsMap.put(key[0].trim(), key[1].trim());
						} 
					}					
				}
			}
			
			setData(sf);
			
			line = readLine(rowDelim);
			
			while (line != null) {
				line = line.trim();
				if (line.length() > 0 || !line.isEmpty()) {
					
					// Checking for the header rows
					if(line.contains(headerDelim) && !line.contains(colDelim)) {
						Matcher m = headerPattern.matcher(line);
						if (m.find()) {
							// Encountered a header row
							readHeader(line);
						}
						else {
							log.fine("Line '" + line + "' contains Header Delimeter, but is not a header. Thus skipping the line.");
						}
					}
					else {
						if (mFile != null) {
							readDataLine(line);
						}
						else {
							log.log(Level.INFO, "File format is incorrect or error occured while parsing for Tag ID," + 
							" as a result of which Header Less data is encountered. Thus skipping the line '" + line + "'");
						}
					}
				}
				
				line = readLine(rowDelim);
			}
			
			if (mFile != null && (lastBlockIdToSkip.isEmpty() || !mFile.getTagID().equals(lastBlockIdToSkip))) {
				// To write the last MO
				writeData();
			}
		} catch (Exception e) {
			log.log(Level.WARNING, "General Failure.", e);
		} finally {
			if (mFile != null) {
				try {
					mFile.close();
				} catch (Exception e) {
					log.log(Level.WARNING, "Error closing MeasurementFile.", e);
		        }
			}
			
			if (br != null) {
		        try {
		          br.close();
		        } catch (Exception e) {
		          log.log(Level.WARNING, "Error closing Reader.", e);
		        }
			}
		}
	}
	
	private void readHeader(String headerLine) {
		try {
			// String headerString = headerLine.substring(0, headerLine.lastIndexOf("-")).trim();
			headerNumber = headerLine.substring(headerLine.lastIndexOf("-") + 1, headerLine.length()).trim();
			if (mFile != null && mFile.hasData()) {
				writeData();
			}
				
			mFile = Main.createMeasurementFile(sf, headerNumber, techPack, setType, setName, this.workerName, log);
		} catch (Exception e) {
			log.log(Level.WARNING, "General failure occured while Parsing for the Tag ID.", e);
			if (mFile == null) {
				try {
					mFile = Main.createMeasurementFile(sf, headerNumber, techPack, setType, setName, this.workerName, log);
				} catch (Exception ex) {
					log.log(Level.WARNING, "Error occured while creating Measurement File.", e);
				}
			}
		}
	}
	
	private void readDataLine(String line) throws Exception {
		String counterName = line.substring(0, line.indexOf(colDelim)).trim().replaceAll("\\s+", " ");			
		String counterValue;
		
		if (keyPairsMap != null && keyPairsMap.containsKey(counterName) && line.split(colDelim).length > 2) {
			String secondKeyValuePair = keyPairsMap.get(counterName) + line.substring(line.lastIndexOf(colDelim));
			readDataLine(secondKeyValuePair);
			
			line = line.substring(line.indexOf(colDelim),line.indexOf(keyPairsMap.get(counterName)));
		}
		
		if (line.length() > line.indexOf(colDelim) + 1) {
			counterValue = line.substring(line.indexOf(colDelim) + 1, line.length()).trim();
			if (counterValue.contains("\t")) {
				String firstVal = counterValue.substring(0, counterValue.indexOf("\t"));
				String secondVal = counterValue.substring(counterValue.lastIndexOf("\t") + 1, counterValue.length());
				counterValue = firstVal + multiValDelim + secondVal;
			}
		}
		else {
			counterValue = "NULL";
		}
		
		if (dateTimeMode == DATETIME_FROM_SINGLE_COLUMN) {
			if (counterName == dateTimeColumn) {
				dateTimeIdData = counterValue;
			}
		}
		
		if (startDateTimeMode == START_DATETIME_FROM_SINGLE_COLUMN) {
			if (counterName == startDateTimeColumn) {
				startDateTimeIdData = counterValue;
			}
		}
		else if (startDateTimeMode == START_DATETIME_FROM_MULTI_COLUMN) {
			if (counterName.equalsIgnoreCase(startDateCol)) {
				startDateTimeIdData = counterValue + " " + startDateTimeIdData;
			}
			else if (counterName.equalsIgnoreCase(startTimeCol)) {
				startDateTimeIdData = startDateTimeIdData + counterValue;
			}
		}
		
		if (calculateEndDateTime.equalsIgnoreCase("true") && counterName.equalsIgnoreCase(totalRecordingTimeCol)) {
			totalRecordingTime = Integer.parseInt(counterValue);
		}
		
		if (mandatoryKeyValuePair != null && mandatoryKeyValuePair.containsKey(counterName)) {
			mandatoryKeyValuePair.put(counterName, counterValue);
		}
		
		if (mFile != null) {
			log.log(Level.FINEST, "Adding " + counterName + ", " + counterValue + " to the Measurement File.");
			mFile.addData(counterName, counterValue);
		} else {
			log.log(Level.FINEST, "Measurement file is not initialized hence " + counterName + 
					", " + counterValue + " will not be written to the Measurement File.");
		}
	}
	
	private void writeData() {
		try {
			mFile.addData("filename", filename);
			mFile.addData("DC_SUSPECTFLAG", "");
			mFile.addData("DIRNAME", sf.getDir());
			mFile.addData("JVM_TIMEZONE", JVM_TIMEZONE);
			if (dateTimeMode == DATETIME_FROM_FILENAME) {
				mFile.addData("DATETIME_ID", dateTimeColumn);
			}
			else if (dateTimeMode == DATETIME_FROM_SINGLE_COLUMN) {
				mFile.addData("DATETIME_ID", dateTimeIdData);
			}
			
			try {
				formatDates(startDateTimeIdData);
			} catch (Exception e) {
				log.log(Level.WARNING, "Formatting Dates failed with Exception.", e);
			}
			
			if (mandatoryKeyValuePair != null) {
				for (String key : mandatoryKeyValuePair.keySet()) {
					mFile.addData(key, mandatoryKeyValuePair.get(key));
				}
			}
			
			log.log(Level.FINEST, "Invoking SaveData for for Measurement File with Tag ID: " + mFile.getTagID());
			mFile.saveData();
			log.log(Level.FINEST, "SaveData executed successfully, closing the Measurement File...");
			mFile.close();
		} catch (Exception e) {
			log.log(Level.WARNING, "General Failure while writing Measurement File.", e);
		}
		finally {
			if (mFile != null) {
				try {
					mFile.close();
				} catch (Exception e) {
					log.log(Level.WARNING, "Error closing MeasurementFile.", e);
		        }
			}
		}
	}
	
	private void formatDates(String dateTime) throws ParseException {
		SimpleDateFormat source = new SimpleDateFormat(sourceDateFormat);
		SimpleDateFormat target = new SimpleDateFormat(targetDateFormat);
		Calendar cal = Calendar.getInstance();
		cal.setTime(source.parse(dateTime));
		mFile.addData("START_DATETIME", target.format(cal.getTime()));
		
		if (calculateEndDateTime.equalsIgnoreCase("true")) {
			cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + totalRecordingTime);
			mFile.addData("END_DATETIME", target.format(cal.getTime()));
		}
	}
	
	/**
	 * Creates new Bufferreader from a file.
	 * 
	 * @param Filename
	 * 
	 */
	private void setData(final SourceFile sf) throws Exception {
		final String charsetName = StaticProperties.getProperty("charsetName", null);
		InputStreamReader isr = null;
		if (charsetName == null) {
			isr = new InputStreamReader(sf.getFileInputStream());
		} else {
			log.log(Level.FINEST, "InputStreamReader charsetName: " + charsetName);
			isr = new InputStreamReader(sf.getFileInputStream(), charsetName);
		}
		log.log(Level.FINEST, "InputStreamReader Encoding: " + isr.getEncoding());
		br = new BufferedReader(isr);
	}

	/**
	 * read characters from reader until eof or delimiter is encountered.
	 * 
	 * @param Filename
	 * 
	 */
	private String readLine(final String delimiter) throws Exception {
		// if end of line return with null
		if (null == this.block) {
			return null;
		}
		
		final char[] tmp = new char[bufferSize];
		while (true) {
			// log.log(Level.FINEST, "buffer: " + block);
			// delimiter found
			final String[] result = this.block.split(delimiter);
			if (result.length > 1) {
				// remove discovered token + deliminator from block
				block = block.substring(result[0].length() + rowDelimLength);
				log.log(Level.FINEST, "result: " + result[0]);
				// return found block
				return result[0];
			} else {
				// delimiter not found, read next block
				final int count = br.read(tmp, 0, bufferSize);
				// if end of file return whole block and set block to null value
				if (count == -1) {
					final String finalBlock = this.block;
					this.block = null;
					return finalBlock;
				}
				this.block += (new String(tmp));
			}
		}

	}

}
