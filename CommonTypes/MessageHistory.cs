using System.Collections.Concurrent;
using System.Collections.Generic;

namespace CommonTypes
{
    public class MessageHistory
    {
        // maps a site's name to a table that maps processes' names to a list of messages
        private IDictionary<string, IDictionary<string, ProcessHistory>> PublicationHistory = new ConcurrentDictionary<string, IDictionary<string, ProcessHistory>>();
        //private IDictionary<string, IDictionary<string, string[]>> SubscriptionHistory;

        /// <summary>
        ///     Stores the publication
        /// </summary>
        /// <param name="siteName"></param>
        /// <param name="message"></param>
        public void StorePublication(string siteName, string[] message)
        {
            lock (this.PublicationHistory)
            {
                string publisher = message[0];
                int seqNo = int.Parse(message[4]);

                // gets or creates the history of messages for the given site
                IDictionary<string, ProcessHistory> procToHistory;
                if (!this.PublicationHistory.TryGetValue(siteName, out procToHistory))
                {
                    procToHistory = new ConcurrentDictionary<string, ProcessHistory>();
                    this.PublicationHistory[siteName] = procToHistory;
                }

                // gets or creates the history of messages for the given publisher
                ProcessHistory procHistory;
                if (!procToHistory.TryGetValue(publisher, out procHistory))
                {
                    procHistory = new ProcessHistory();
                    procToHistory[publisher] = procHistory;
                }

                // adds the message to the process' history
                procHistory.AddMessage(message, seqNo);
                
                //Console.Out.WriteLine("Storing message for "+siteName);
            }
        }

        /// <summary>
        ///     Retrieves the publication
        /// </summary>
        /// <param name="siteName"></param>
        /// <param name="publisher"></param>
        /// <param name="sequenceNumber"></param>
        /// <returns></returns>
        public string[] GetPublication(string siteName, string publisher, int sequenceNumber)
        {
            // gets the history of messages for the given site and publisher

            IDictionary<string, ProcessHistory> procToHistory;
            ProcessHistory procHistory;
            if (this.PublicationHistory.TryGetValue(siteName, out procToHistory) && 
                procToHistory.TryGetValue(publisher, out procHistory))
            {
                return procHistory.GetMessage(sequenceNumber);
            }

            return null;
        }
    }
}
