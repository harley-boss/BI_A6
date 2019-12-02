/// File:        DatabaseManager.cs
/// Assignment:  A6 Big Data
/// Application: SurveyParser
/// Class:       Business Intelligence
/// Programmers: Harley Boss & Justin Struk
/// Date:        December 2nd 2019
/// Description: This file handles connection to and insertion of data into the SQL server
///              database running locally



using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace SurveyParser {



    /// <summary>
    /// Class responsible for handling all the database queries
    /// </summary>
    class DatabaseManager {

        private SqlConnection sqlConnection;
        private String connString;
        private String databaseName = "2011Survey";
        private String tableName = "Data";




        /// <summary>
        /// Instantiates a database connection either with integrated security or a username and
        /// password if provided
        /// </summary>
        /// <param name="machineName">Machine name of the computer to connect to</param>
        /// <param name="userName">Username if required</param>
        /// <param name="password">Password if required</param>
        public DatabaseManager(String machineName, String userName = "", String password = "") {
            connString = "Data Source=" + machineName + ";Initial Catalog=" + databaseName + ";";
            if (userName == "" || password == "") {
                connString += "Integrated Security=True";
            } else {
                connString += "User Id=" + userName + ";password=" + password + ";Trusted_Connection=False;";
            }
            sqlConnection = new SqlConnection(connString);
        }




        /// <summary>
        /// Tests a connection to the database
        /// </summary>
        /// <returns>True if connection has been established</returns>
        public Boolean IsConnected() {
            Boolean isConnected = true;
            try {
                sqlConnection.Open();
            } catch (SqlException sql) {
                Console.WriteLine("Error: " + sql.Message);
                isConnected = false;
            } catch (Exception e) {
                Console.WriteLine("Error: " + e.Message);
                isConnected = false;
            } finally {
                sqlConnection.Close();
            }
            return isConnected;
        }




        /// <summary>
        /// Inserts thousands of records at a time into the database
        /// </summary>
        /// <param name="entities">List of DataEntiity objects</param>
        /// <returns>True if insert was successful, false otherwise</returns>
        public bool BulkInsert(List<DataEntity> entities) {
            // connect to SQL
            DataTable dt = new DataTable();
            dt.Columns.Add("GEO");
            dt.Columns.Add("Sex");
            dt.Columns.Add("AGEGRS");
            dt.Columns.Add("NOC2011");
            dt.Columns.Add("COWD");
            dt.Columns.Add("Value");
            DataRow dr;
            foreach (DataEntity e in entities) {
                dr = dt.NewRow();
                dr["GEO"] = e.GEO;
                dr["Sex"] = e.Sex;
                dr["AGEGRS"] = e.AGEGRS;
                dr["NOC2011"] = e.NOC2011;
                dr["COWD"] = e.COWD;
                dr["Value"] = e.Value;
                dt.Rows.Add(dr);
            }

            try {
                using (SqlConnection connection = new SqlConnection(connString)) {
                    // make sure to enable triggers
                    // more on triggers in next post
                    SqlBulkCopy bulkCopy = new SqlBulkCopy(
                        connection,
                        SqlBulkCopyOptions.TableLock |
                        SqlBulkCopyOptions.FireTriggers |
                        SqlBulkCopyOptions.UseInternalTransaction,
                        null
                        );

                    // set the destination table name
                    bulkCopy.DestinationTableName = this.tableName;
                    connection.Open();

                    // write the data in the "dataTable"
                    bulkCopy.WriteToServer(dt);
                    connection.Close();
                }
                // reset
                dt.Clear();
                return true;
            } catch (SqlException sqe) {
                Console.WriteLine(sqe.Message);
                return false;
            }
        }




        /// <summary>
        /// Inserts a Geography description into the database
        /// </summary>
        /// <param name="id">Id of object</param>
        /// <param name="desc">Description of object</param>
        /// <returns>int of the row number where record was inserted</returns>
        public int InsertGeo(string id, string desc) {
            int numRows = 0;
            using (SqlConnection openCon = new SqlConnection(connString)) {
                string saveStaff = "INSERT into GEO (GeoID,description) VALUES (@GeoID,@description)";

                using (SqlCommand querySaveGeo = new SqlCommand(saveStaff)) {
                    querySaveGeo.Connection = openCon;
                    querySaveGeo.Parameters.Add("@GeoID", SqlDbType.VarChar, 50).Value = id;
                    querySaveGeo.Parameters.Add("@description", SqlDbType.NVarChar, 50).Value = desc;

                    openCon.Open();

                    try {
                        numRows = querySaveGeo.ExecuteNonQuery();
                    } catch (SqlException e) {
                        Console.WriteLine("Error: {0}", e.Message);
                    }
                }
            }
            return numRows;
        }




        /// <summary>
        /// Inserts an Agegroup entry into the database
        /// </summary>
        /// <param name="id">Id of the object</param>
        /// <param name="desc">Description of the object</param>
        /// <returns>int of the row number where record was inserted</returns>
        public int InsertAgeGroup(string id, string desc) {
            int numRows = 0;
            using (SqlConnection openCon = new SqlConnection(connString)) {
                string saveStaff = "INSERT into AgeGroup (GroupID,description) VALUES (@GroupID,@description)";

                using (SqlCommand querySaveGeo = new SqlCommand(saveStaff)) {
                    querySaveGeo.Connection = openCon;
                    querySaveGeo.Parameters.Add("@GroupID", SqlDbType.VarChar, 50).Value = id;
                    querySaveGeo.Parameters.Add("@description", SqlDbType.NVarChar, 50).Value = desc;

                    openCon.Open();

                    try {
                        numRows = querySaveGeo.ExecuteNonQuery();
                    } catch (SqlException e) {
                        Console.WriteLine("Error: {0}", e.Message);
                    }
                }
            }
            return numRows;
        }




        /// <summary>
        /// Inserts a Sex entry into the database
        /// </summary>
        /// <param name="id">Id of the object</param>
        /// <param name="desc">Description of the object</param>
        /// <returns>int of the row number where record was inserted</returns>
        public int InsertSex(string id, string desc) {
            int numRows = 0;
            using (SqlConnection openCon = new SqlConnection(connString)) {
                string saveStaff = "INSERT into SEX (SexID,description) VALUES (@SexID,@description)";

                using (SqlCommand querySaveGeo = new SqlCommand(saveStaff)) {
                    querySaveGeo.Connection = openCon;
                    querySaveGeo.Parameters.Add("@SexID", SqlDbType.VarChar, 50).Value = id;
                    querySaveGeo.Parameters.Add("@description", SqlDbType.VarChar, 50).Value = desc;

                    openCon.Open();

                    try {
                        numRows = querySaveGeo.ExecuteNonQuery();
                    } catch (SqlException e) {
                        Console.WriteLine("Error: {0}", e.Message);
                    }
                }
            }
            return numRows;
        }




        /// <summary>
        /// Inserts a NOC entry into the database
        /// </summary>
        /// <param name="id">Id of the object</param>
        /// <param name="desc">Description of the object</param>
        /// <returns>int of the row number where record was inserted</returns>
        public int InsertNOC(string id, string desc) {
            int numRows = 0;
            using (SqlConnection openCon = new SqlConnection(connString)) {
                string saveStaff = "INSERT into NOC (NocID,description) VALUES (@NocID,@description)";

                using (SqlCommand querySaveGeo = new SqlCommand(saveStaff)) {
                    querySaveGeo.Connection = openCon;
                    querySaveGeo.Parameters.Add("@NocID", SqlDbType.VarChar, 50).Value = id;
                    querySaveGeo.Parameters.Add("@description", SqlDbType.NVarChar, 50).Value = desc;

                    openCon.Open();

                    try {
                        numRows = querySaveGeo.ExecuteNonQuery();
                    } catch (SqlException e) {
                        Console.WriteLine("Error: {0}", e.Message);
                    }
                }
            }
            return numRows;
        }
    }
}
