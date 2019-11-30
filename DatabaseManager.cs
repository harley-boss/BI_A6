using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace SurveyParser
{
    class DatabaseManager
    {

        private SqlConnection sqlConnection;
        private String connString;
        private String databaseName = "2011Survey";
        private String tableName = "Data";


        public DatabaseManager(String machineName, String userName = "", String password = "")
        {
            connString = "Data Source=" + machineName + ";Initial Catalog=" + databaseName + ";";
            if (userName == "" || password == "")
            {
                connString += "Integrated Security=True";
            }
            else
            {
                connString += "User Id=" + userName + ";password=" + password + ";Trusted_Connection=False;";
            }
            sqlConnection = new SqlConnection(connString);
        }

        public Boolean IsConnected()
        {
            Boolean isConnected = true;
            try
            {
                sqlConnection.Open();
            }
            catch (SqlException sql)
            {
                Console.WriteLine("Error: " + sql.Message);
                isConnected = false;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
                isConnected = false;
            }
            finally
            {
                sqlConnection.Close();
            }
            return isConnected;
        }

        public bool BulkInsert(List<DataEntity> entities)
        {
            // connect to SQL
            DataTable dt = new DataTable();
            dt.Columns.Add("GEO");
            dt.Columns.Add("Sex");
            dt.Columns.Add("AGEGRS");
            dt.Columns.Add("NOC2011");
            dt.Columns.Add("COWD");
            dt.Columns.Add("Value");
            DataRow dr;
            foreach (DataEntity e in entities)
            {
                dr = dt.NewRow();
                dr["GEO"] = e.GEO;
                dr["Sex"] = e.Sex;
                dr["AGEGRS"] = e.AGEGRS;
                dr["NOC2011"] = e.NOC2011;
                dr["COWD"] = e.COWD;
                dr["Value"] = e.Value;
                dt.Rows.Add(dr);
            }

            try
            {
                using (SqlConnection connection = new SqlConnection(connString))
                {
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
            }
            catch (SqlException sqe)
            {
                Console.WriteLine(sqe.Message);
                return false;
            }
        }


        public int insertGeo(string id, string desc)
        {
            int numRows = 0;
            using (SqlConnection openCon = new SqlConnection(connString))
            {
                string saveStaff = "INSERT into GEO (GeoID,description) VALUES (@GeoID,@description)";

                using (SqlCommand querySaveGeo = new SqlCommand(saveStaff))
                {
                    querySaveGeo.Connection = openCon;
                    querySaveGeo.Parameters.Add("@GeoID", SqlDbType.NChar, 10).Value = id;
                    querySaveGeo.Parameters.Add("@description", SqlDbType.NVarChar, 50).Value = desc;

                    openCon.Open();

                    try
                    {
                        numRows = querySaveGeo.ExecuteNonQuery();
                    }
                    catch (SqlException e)
                    {
                        Console.WriteLine("Error: {0}" , e.Message);
                    }

                }
            }
            return numRows;
        }

        public int insertAgeGroup(string id, string desc)
        {
            int numRows = 0;
            using (SqlConnection openCon = new SqlConnection(connString))
            {
                string saveStaff = "INSERT into AgeGroup (GroupID,description) VALUES (@GroupID,@description)";

                using (SqlCommand querySaveGeo = new SqlCommand(saveStaff))
                {
                    querySaveGeo.Connection = openCon;
                    querySaveGeo.Parameters.Add("@GroupID", SqlDbType.NChar, 10).Value = id;
                    querySaveGeo.Parameters.Add("@description", SqlDbType.NVarChar, 50).Value = desc;

                    openCon.Open();

                    try
                    {
                        numRows = querySaveGeo.ExecuteNonQuery();
                    }
                    catch (SqlException e)
                    {
                        Console.WriteLine("Error: {0}", e.Message);
                    }

                }
            }
            return numRows;
        }
    }
}
