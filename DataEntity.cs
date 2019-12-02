/// File:        DataEntity.cs
/// Assignment:  A6 Big Data
/// Application: SurveyParser
/// Class:       Business Intelligence
/// Programmers: Harley Boss & Justin Struk
/// Date:        December 2nd 2019
/// Description: A shallow class representing a data entry from the survey .xml file


using System;

namespace SurveyParser {
    class DataEntity {
		public String GEO { get; set; }

		public String Sex { get; set; }

		public String AGEGRS { get; set; }

		public String NOC2011 { get; set; }

		public String COWD { get; set; }

		public int Value { get; set; }

        public DataEntity Build(String data) {
            String[] elements = data.Split(';');
            this.GEO = elements[0];
            this.Sex = elements[1];
            this.AGEGRS = elements[2];
            this.NOC2011 = elements[3];
            this.COWD = elements[4];
            this.Value = Int32.Parse(elements[5]);
            return this;
        }
    }
}
