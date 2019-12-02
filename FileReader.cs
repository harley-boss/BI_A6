/// File:        FileReader.cs
/// Assignment:  A6 Big Data
/// Application: SurveyParser
/// Class:       Business Intelligence
/// Programmers: Harley Boss & Justin Struk
/// Date:        December 2nd 2019
/// Description: This file handles reading and creating entries (DataEntity) for all the entries
///              in the xml document housing census data from 2011

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace SurveyParser {
    class FileReader {
        private DatabaseManager dbm;
        private List<DataEntity> failedEntries;



        /// <summary>
        /// Constructor
        /// </summary>
        public FileReader() {
            dbm = new DatabaseManager(Environment.MachineName);
            failedEntries = new List<DataEntity>();
        }




        /// <summary>
        /// Opens the file and reads to the end creating entries based on known tags
        /// </summary>
        public void HandleFile() {
            List<DataEntity> entries = new List<DataEntity>();
            String currentLine = "";
            try {
                using (StreamReader reader = new StreamReader("../../2011_data.xml")) {
                    Console.WriteLine("Loading records from XML file...");
                    int counter = 0;
                    while ((currentLine = reader.ReadLine()) != null) {
                        if (currentLine.Contains("<generic:Series>")) {
                            DataEntity data = ProcessRecord(reader);
                            if (data != null) {
                                counter++;
                                entries.Add(data);
                            }
                            if (counter % 3000 == 0) {
                                if (dbm.IsConnected()) {
                                    if (dbm.BulkInsert(entries)) {
                                        entries.Clear();
                                    } else {
                                        failedEntries.AddRange(entries);
                                        entries.Clear();

                                        if (failedEntries.Count > 200000) {
                                            int count = failedEntries.Count;
                                            Console.Clear();
                                            Console.WriteLine($"Currently have {count} entries that failed to write to the database");
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (entries.Count() > 0) {
                        dbm.BulkInsert(entries);
                    }
                    reader.Close();
                    Console.WriteLine($"Read {counter} lines from file");
                }
            } catch (Exception e) {
                Console.WriteLine("Caught exception when reading from file: " + e.Message);
                Console.WriteLine("\n\nPress any key to exit program...");
                Console.ReadKey();
            }
        }



        /// <summary>
        /// Parses the stream passed in to pull out all values relevant to an entry
        /// </summary>
        /// <param name="stream">Current stream</param>
        /// <returns>DataEntity</returns>
        private DataEntity ProcessRecord(StreamReader stream) {
            DataEntity entity = new DataEntity();
            try {     
                String temp;

                // Discard the first entry
                stream.ReadLine();
                temp = stream.ReadLine();
                entity.GEO = getValue(temp);

                temp = stream.ReadLine();
                entity.Sex = getValue(temp);

                temp = stream.ReadLine();
                entity.AGEGRS = getValue(temp);

                temp = stream.ReadLine();
                entity.NOC2011 = getValue(temp);

                temp = stream.ReadLine();
                entity.COWD = getValue(temp);

                stream.ReadLine();
                stream.ReadLine();
                stream.ReadLine();
                entity.Value = Int32.Parse(getValue(stream.ReadLine()));
            } catch (Exception ex) {
                Console.WriteLine("Caught an exception parsing a record: " + ex.Message);
                entity = null;
            }
            return entity;
        }




        /// <summary>
        /// Current line of the text file is passed in for parsing. Based on known values of the
        /// string, the pertinent data is returned
        /// </summary>
        /// <param name="tag">Current line of the .xml file</param>
        /// <returns>String</returns>
        private String getValue(String tag) {
            String tempString = tag.Trim(' ');
            int idStart = tempString.IndexOf("value=") + 7;
            int idEnd = tempString.IndexOf(" />") - idStart + 1;
            return tempString.Substring(idStart, idEnd).Replace("\"", "");
        }
    }
}
