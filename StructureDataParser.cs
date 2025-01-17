﻿/// File:        StuctureDataParser.cs
/// Assignment:  A6 Big Data
/// Application: SurveyParser
/// Class:       Business Intelligence
/// Programmers: Harley Boss & Justin Struk
/// Date:        December 2nd 2019
/// Description: This file hanles parsing an xml file to obtain all the key references
///              needed for properly identifying the data from the main data document



using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml.Linq;

namespace SurveyParser {
    class StructureDataParser {
        DatabaseManager dbManager;

        public StructureDataParser() {
            dbManager = new DatabaseManager(Environment.MachineName);
        }



        /// <summary>
        /// Method with to parse the xml file and otain all key values
        /// </summary>
        public void ParseStructureFile() {
            XElement root = XElement.Load(@"..\..\structure.xml");

            IEnumerable<XElement> elements = root.Elements();

            XElement cl = elements.ElementAt(1);

            foreach (XElement element in cl.Elements()) {
                string listId = element.FirstAttribute.Value;

                if (listId == "CL_GEO")  {
                    //xml element for geo tags, parse and insert into geo database table
                    foreach (XElement e in element.Elements()) {
                        if (e.HasAttributes)  {
                            string id = e.FirstAttribute.Value;
                            if (e.HasElements) {
                                string desc = e.Elements().ElementAt(0).Value;
                                desc = desc.Trim();

                                //remove leading, trailing, and extra whitespace
                                RegexOptions options = RegexOptions.None;
                                Regex regex = new Regex("[ ]{2,}", options);
                                desc = regex.Replace(desc, " ");

                                dbManager.InsertGeo(id, desc);
                            }
                        }
                    }
                    Console.WriteLine("Added all codes from CL_GEO code list to DB");
                } else if (listId == "CL_AGEGR5") {
                    foreach (XElement e in element.Elements()) {
                        string id = e.FirstAttribute.Value;
                        int idInt;
                        if (int.TryParse(id, out idInt)) {
                            //all AgeGroup IDs are ints, so try to convert to ensure it's an AgeGroup element we're parsing.
                            //Since the db uses nchar(10), it's fixed-length string so don't convert when inserting to the database
                            //to keep the id exactly as it came from the file (i.e. converting "01" to int would result in "1" which wouldn't
                            //match our data table
                            string desc = e.Value;
                            desc = desc.Trim();

                            RegexOptions options = RegexOptions.None;
                            Regex regex = new Regex("[ ]{2,}", options);
                            desc = regex.Replace(desc, " ");

                            dbManager.InsertAgeGroup(id, desc);
                            //Since the id is an int this in an actual age group, let's parse the desc and insert into db
                        }
                    }
                    Console.WriteLine("Added all codes from CL_AGEGR5 code list to DB");
                } else if (listId == "CL_SEX") {
                    foreach (XElement e in element.Elements()) {
                        string id = e.FirstAttribute.Value;
                        int idInt;
                        if (int.TryParse(id, out idInt)) {
                            //all AgeGroup IDs are ints, so try to convert to ensure it's an AgeGroup element we're parsing.
                            //Since the db uses nchar(10), it's fixed-length string so don't convert when inserting to the database
                            //to keep the id exactly as it came from the file (i.e. converting "01" to int would result in "1" which wouldn't
                            //match our data table
                            string desc = e.Value;
                            desc = desc.Trim();

                            RegexOptions options = RegexOptions.None;
                            Regex regex = new Regex("[ ]{2,}", options);
                            desc = regex.Replace(desc, " ");

                            dbManager.InsertSex(id, desc);
                            //Since the id is an int this in an actual age group, let's parse the desc and insert into db
                        }
                    }
                    Console.WriteLine("Added all codes from CL_AGEGR5 code list to DB");
                } else if (listId == "CL_NOC2011") {
                    foreach (XElement e in element.Elements()) {
                        string id = e.FirstAttribute.Value;
                        int idInt;
                        if (int.TryParse(id, out idInt)) {
                            //all AgeGroup IDs are ints, so try to convert to ensure it's an AgeGroup element we're parsing.
                            //Since the db uses nchar(10), it's fixed-length string so don't convert when inserting to the database
                            //to keep the id exactly as it came from the file (i.e. converting "01" to int would result in "1" which wouldn't
                            //match our data table
                            string desc = e.Value;
                            desc = desc.Trim();

                            RegexOptions options = RegexOptions.None;
                            Regex regex = new Regex("[ ]{2,}", options);
                            desc = regex.Replace(desc, " ");

                            dbManager.InsertNOC(id, desc);
                            //Since the id is an int this in an actual age group, let's parse the desc and insert into db
                        }
                    }
                    Console.WriteLine("Added all codes from CL_AGEGR5 code list to DB");
                }
            }
            Console.WriteLine("Finished Parsing the structure.xml file");
        }
    }
}
