using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
