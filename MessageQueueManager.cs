using System;
using System.Collections.Generic;
using System.Messaging;

namespace SurveyParser {
    class MessageQueueManager {

        private static MessageQueue queue;
        private static Boolean isReading;
        private static String queueName;
        private List<DataEntity> dataEntities;
        private DatabaseManager dbm;

        /// <summary>
        /// Initialize the variables of the class
        /// </summary>
        public MessageQueueManager() {
            queueName = "\\private$\\surveydata";
            queue = new MessageQueue();
            queue.Formatter = new ActiveXMessageFormatter();
            queue.MessageReadPropertyFilter.LookupId = true;
            queue.ReceiveCompleted += new ReceiveCompletedEventHandler(QueueUpdated);
            dataEntities = new List<DataEntity>();
            dbm = new DatabaseManager(Environment.MachineName);
        }




        /// <summary>
        /// Called to start reading from the message queue. If the manager is already reading
        /// a subsequent beginRecieve is not called
        /// </summary>
        public void Start() {
            if (isReading) {
                Console.WriteLine("Message queue has already started");
                return;
            }
            isReading = true;
            queue.Path = "Formatname:Direct=os:" + Environment.MachineName + queueName;
            queue.BeginReceive();
        }




        /// <summary>
        /// Stops reading from the message queue
        /// </summary>
        public void Stop() {
            if (!isReading) {
                Console.WriteLine("Message queue has already stopped");
            }
            isReading = false;
        }




        /// <summary>
        /// Handles updates from the message queue. The sender args are parsed to pull out the 
        /// data to construct a new yoyo object. That data is then inserted into the database
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void QueueUpdated(object sender, ReceiveCompletedEventArgs e) {
            try {
                String data = e.Message.Body.ToString();
                DataEntity dataEntity = new DataEntity();
                dataEntity.Build(data);
                dataEntities.Add(dataEntity);
                if (dataEntities.Count > 1000) {
                    List<DataEntity> tempEntities = dataEntities;          
                    dbm.BulkInsert(tempEntities);
                    dataEntities.Clear();
                }
                if (isReading) {
                    queue.BeginReceive();
                }
            } catch (Exception ex) {
                Console.WriteLine("Caught exception :" + ex.Message);
            }
        }



        public void SendToMessageQueue(DataEntity entity) {
            String data = entity.GEO + ";"
                + entity.Sex + ";"
                + entity.AGEGRS + ";"
                + entity.NOC2011 + ";"
                + entity.COWD + ";"
                + entity.Value;
            try {
                queue.Send(data, "Entity");
            } catch (MessageQueueException mqe) {
                Console.WriteLine("Message Queue Exception: " + mqe.Message);
            }
        }
    }
}
