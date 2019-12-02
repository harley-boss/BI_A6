/// File:        MainLine.cs
/// Assignment:  A6 Big Data
/// Application: SurveyParser
/// Class:       Business Intelligence
/// Programmers: Harley Boss & Justin Struk
/// Date:        December 2nd 2019
/// Description: This is the main of the application



namespace SurveyParser {
    class MainLine { 

        /// <summary>
        /// Launching point of the application
        /// </summary>
        /// <param name="args">Command line arguements</param>
        static void Main(string[] args) {
            FileReader reader = new FileReader();
            reader.HandleFile();
            StructureDataParser structureparser = new StructureDataParser();
            structureparser.ParseStructureFile();
        }
    }
}
