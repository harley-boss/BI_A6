﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SurveyParser {
    class MainLine { 
        static void Main(string[] args) {
            FileReader reader = new FileReader();
            reader.HandleFile();
            StructureDataParser structureparser = new StructureDataParser();
            structureparser.parseStructureFile();
        }
    }
}
