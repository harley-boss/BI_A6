using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace SurveyParser
{
    class StructureDataParser
    {
        DatabaseManager dbManager;

        public StructureDataParser()
        {
            dbManager = new DatabaseManager(Environment.MachineName);
        }
        public void parseStructureFile()
        {
            XElement root = XElement.Load(@"..\..\structure.xml");

            IEnumerable<XElement> elements = root.Elements();

            XElement cl = elements.ElementAt(1);

            foreach (XElement element in cl.Elements())

            {
                string listId = element.FirstAttribute.Value;

                if (listId == "CL_GEO")
                {
                    //xml element for geo tags, parse and insert into geo database table
                    foreach (XElement e in element.Elements())
                    {
                        if (e.HasAttributes)
                        {
                            string id = e.FirstAttribute.Value;
                            if (e.HasElements)
                            {
                                string desc = e.Elements().ElementAt(0).Value;
                                desc = desc.Trim();

                                //remove leading, trailing, and extra whitespace
                                RegexOptions options = RegexOptions.None;
                                Regex regex = new Regex("[ ]{2,}", options);
                                desc = regex.Replace(desc, " ");

                                dbManager.insertGeo(id, desc);
                                //Console.WriteLine("ID: " + idInt + " Desc: " + desc);
                            }
                        }
                    }
                    Console.WriteLine("Added all codes from CL_GEO code list to DB");
                }
                else if (listId == "CL_AGEGR5")
                {
                    foreach (XElement e in element.Elements())
                    {
                        string id = e.FirstAttribute.Value;
                        int idInt;
                        if (int.TryParse(id, out idInt))
                        {
                            //all AgeGroup IDs are ints, so try to convert to ensure it's an AgeGroup element we're parsing.
                            //Since the db uses nchar(10), it's fixed-length string so don't convert when inserting to the database
                            //to keep the id exactly as it came from the file (i.e. converting "01" to int would result in "1" which wouldn't
                            //match our data table
                            string desc = e.Value;
                            desc = desc.Trim();

                            RegexOptions options = RegexOptions.None;
                            Regex regex = new Regex("[ ]{2,}", options);
                            desc = regex.Replace(desc, " ");

                            dbManager.insertAgeGroup(id, desc);
                            //Console.WriteLine("Age Group: " + idInt + " Desc: " + desc);
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
