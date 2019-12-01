using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SurveyParser {
    class FileReader {
        private DatabaseManager dbm;
        private List<DataEntity> failedEntries;

        public FileReader() {
            dbm = new DatabaseManager(Environment.MachineName);
            failedEntries = new List<DataEntity>();

        }

        // Gets all entries from the file - assumption is the file is located in the same directory
        // as the solution
        public void HandleFile() {
            List<DataEntity> entries = new List<DataEntity>();
            String currentLine = "";
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
        }

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

                // Need this value? -- doesn't seem to be getting parsed correctly

                stream.ReadLine();
                stream.ReadLine();
                stream.ReadLine();
                entity.Value = Int32.Parse(getValue(stream.ReadLine()));
            } catch (Exception ex) {
                entity = null;
            }
            return entity;
        }

        private String getValue(String tag) {
            String tempString = tag.Trim(' ');
            int idStart = tempString.IndexOf("value=") + 7;
            int idEnd = tempString.IndexOf(" />") - idStart + 1;
            return tempString.Substring(idStart, idEnd).Replace("\"", "");
        }
    }
}
